package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.feeder.DataFeeder;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.core.serde.SerdePrototype;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.javatuples.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This stream program is able to recognize accidents.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class AccidentDetectionStreamBuilder extends StreamBuilder<Triplet<Integer, Integer, Boolean>, Long> {

    public static final String TOPIC = "ACC";

    private static final Logger logger = LoggerFactory
            .getLogger(AccidentDetectionStreamBuilder.class);

    @Autowired
    public AccidentDetectionStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util, new TupleSerdes.TripletSerdes<>(), new ValueSerde());
    }

    @Override
    protected KStream<Triplet<Integer, Integer, Boolean>, Long> getStream(KStreamBuilder builder) {
        logger.debug("Building stream to identify accidents");

        // an accident at minute m expressway x, segment s, direction d will be mapped to all segments downstream 0..4
        //detect an accident on a given segment whenever two or more vehicles are stopped in that segment at the same lane and position
        // therefore flatmapping to all affected segments
        KStream<Pair<Long, Integer>, Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> source1 =
                builder.stream(new TupleSerdes.PairSerdes<>(),
                        new TupleSerdes.SextetSerdes<>(), PositionReportHandler.TOPIC);

        return source1.filter((k, v) -> v.getValue0() == 0).map((key, value) -> new KeyValue<>(
                //  XWay, Lane, Dir, Seg, Pos
                new Quintet<>(value.getValue1(), value.getValue2(), value.getValue3(), value.getValue4(), value.getValue5()),
                // VID , minute
                new Pair<>(key.getValue1(), Util.minuteOfReport(key.getValue0()))))
                // current time to use | if more than one vehicle in window | current count of position reports in window

                .aggregateByKey(() -> new Quartet<>(0l, -1, false, 0),
                        (key, value, aggregat) -> {

                            long time = value.getValue1() > aggregat.getValue0() ? value.getValue1() : aggregat.getValue0();
                            // indicates if there are multiple cars in the considered window
                            boolean multiple = aggregat.getValue2() ||
                                    (aggregat.getValue1() != -1 && (!aggregat.getValue1().equals(key.getValue0())));
                            return new Quartet<>(time, value.getValue0(), multiple, aggregat.getValue3() + 1);
                        }
                        , TimeWindows.of("window", 4 * 30).advanceBy(30)).toStream()
                .filter((k, v) -> v.getValue2() && v.getValue3() >=8)
                // key -> xway, segment, direction | value -> minute in which accident has been detected
                .flatMap((key0, value0) -> IntStream.of(4).mapToObj(in -> new KeyValue<>(new Triplet<>(key0.key().getValue0(), key0.key().getValue3()-in, key0.key().getValue2()), value0.getValue0())).collect(Collectors.toList()));


    }

    @Override
    protected String getOutputTopic() {
        return TOPIC;
    }




    public static class ValueSerde extends SerdePrototype<Long> {
        public ValueSerde() {
            super(new Serdes.LongSerde());
        }
    }
}
