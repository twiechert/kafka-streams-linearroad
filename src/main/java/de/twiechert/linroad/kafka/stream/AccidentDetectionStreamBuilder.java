package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.javatuples.Pair;
import org.javatuples.Quintet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This stream program is able to recognize accidents.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class AccidentDetectionStreamBuilder {


    private static final Logger logger = LoggerFactory
            .getLogger(AccidentDetectionStreamBuilder.class);

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public KStream<XwaySegmentDirection, Long> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify accidents");
        TimeWindows accDetectionWindow = TimeWindows.of(context.topic("ACC_DET_WINDOW"), 4 * 30).advanceBy(30);

        // an accident at minute m expressway x, segment s, direction d will be mapped to all segments downstream 0..4
        //detect an accident on a given segment whenever two or more vehicles are stopped in that segment at the same lane and position
        // therefore flatmapping to all affected segments

        return positionReportStream.filter((k, v) -> v.getSpeed() == 0).map((key, value) -> new KeyValue<>(
                new XwayLaneDirSegPosIntermediate(key.getXway(), value.getLane(), key.getDir(), key.getSeg(), value.getPos()),
                new Pair<>(value.getTime(), value.getVehicleId())))
                // current time to use | if more than one vehicle in window | current count of position reports in window

                .aggregateByKey(() -> new AccidentDetectionValIntermediate(0L, new HashMap<>()),
                        (key, value, aggregat) -> {
                            if (!aggregat.getVehicleMap().containsKey(value.getValue1())) {
                                aggregat.getVehicleMap().put(value.getValue1(), 1);
                            } else {
                                aggregat.getVehicleMap().put(value.getValue1(), aggregat.getVehicleMap().get(value.getValue1()) + 1);
                            }
                            // the latest timestamp is updated again, which is required by the punctuator to work properply
                            return new AccidentDetectionValIntermediate(value.getValue0(), aggregat.getVehicleMap());
                        }
                        , accDetectionWindow, new DefaultSerde<>(), new DefaultSerde<>())
                .toStream()
                /**
                 * There must be at least two cars emitting four consecutive position reports.
                 */
                .filter((k, v) -> v.getVehicleMap().entrySet().stream().filter(p -> p.getValue() >= 4).count() >= 2)


                // key -> xway, segment, direction | value -> minute in which accident has been detected
                .flatMap((key, value0) ->
                        IntStream.of(4).mapToObj(in -> new KeyValue<>(new XwaySegmentDirection(key.key().getValue0(), ((key.key().getValue3() - in) < 0) ? 0 : key.key().getValue3() - in, key.key().getValue2()),
                                Util.minuteOfReport(key.window().end()))).collect(Collectors.toList()))
                .through(new DefaultSerde<>(), new DefaultSerde<>(), context.topic("ACC_DETECTION"));


        //  return OnMinuteChangeEmitter.getForSliding(context.getBuilder(), accDetectionStream, accDetectionWindow, new DefaultSerde<>(), new DefaultSerde<>(), "LATEST_ACC")

    }


    public static class AccidentDetectionValIntermediate extends Pair<Long, HashMap<Integer, Integer>> {

        public AccidentDetectionValIntermediate(Long time, HashMap<Integer, Integer> vehicles) {
            super(time, vehicles);
        }

        public HashMap<Integer, Integer> getVehicleMap() {
            return this.getValue1();
        }
    }

    public static class XwayLaneDirSegPosIntermediate extends Quintet<Integer, Integer, Boolean, Integer, Integer> {
        public XwayLaneDirSegPosIntermediate(Integer xway, Integer lane, Boolean dir, Integer segment, Integer pos) {
            super(xway, lane, dir, segment, pos);
        }


    }

}
