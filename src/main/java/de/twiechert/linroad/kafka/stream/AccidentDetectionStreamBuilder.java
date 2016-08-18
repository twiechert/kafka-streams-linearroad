package de.twiechert.linroad.kafka.stream;

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


    public KStream<XwaySegmentDirection, Long> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify accidents");

        // an accident at minute m expressway x, segment s, direction d will be mapped to all segments downstream 0..4
        //detect an accident on a given segment whenever two or more vehicles are stopped in that segment at the same lane and position
        // therefore flatmapping to all affected segments


        // IMPORTANT for joining: , but only if q (position report) was emitted
        // **no** earlier than the minute following the minute when the accident occurred.
        // i.e. the accident detection must be "before" up to one second
        // --> we map accidents of minutes k to minute k+1
        return positionReportStream.filter((k, v) -> v.getSpeed() == 0).map((key, value) -> new KeyValue<>(
                new XwayLaneDirSegPosIntermediate(key.getXway(), value.getLane(), key.getDir(), key.getSeg(), value.getPos()),
                new Pair<>(value.getTime(), value.getVehicleId())))
                // current time to use | if more than one vehicle in window | current count of position reports in window

                .aggregateByKey(AccidentDetectionValIntermediate::new,
                        (key, value, aggregat) -> {
                            if (!aggregat.containsKey(value.getValue1())) {
                                aggregat.put(value.getValue1(), 1);
                            } else {
                                aggregat.put(value.getValue1(), aggregat.get(value.getValue1()) + 1);
                            }
                            return aggregat;
                        }
                        , TimeWindows.of("ACC-DET-WINDOW", 4 * 30).advanceBy(30), new DefaultSerde<>(), new DefaultSerde<>())
                .toStream()
                .filter((k, v) -> v.entrySet().stream().filter(p -> p.getValue() >= 4).count() >= 2)
                // key -> xway, segment, direction | value -> minute in which accident has been detected
                .flatMap((key0, value0) ->
                        IntStream.of(4).mapToObj(in -> new KeyValue<>(new XwaySegmentDirection(key0.key().getValue0(), ((key0.key().getValue3() - in) < 0) ? 0 : key0.key().getValue3() - in, key0.key().getValue2()),
                                Util.minuteOfReport(key0.window().end()) + 1)).collect(Collectors.toList()));



    }


    public static class AccidentDetectionValIntermediate extends HashMap<Integer, Integer> {


    }

    public static class XwayLaneDirSegPosIntermediate extends Quintet<Integer, Integer, Boolean, Integer, Integer> {
        public XwayLaneDirSegPosIntermediate(Integer value0, Integer value1, Boolean value2, Integer value3, Integer value4) {
            super(value0, value1, value2, value3, value4);
        }


    }

}
