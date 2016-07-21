package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.feeder.DataFeeder;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.core.serde.SerdePrototype;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;


/**
 * This class builds the number of vehicles that per (expressway, segment, direction) tuple.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class NumberOfVehiclesStreamBuilder extends StreamBuilder<Triplet<Integer, Integer, Boolean>, Pair<Long, Integer>> {

    public static final String TOPIC = "NOV";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(NumberOfVehiclesStreamBuilder.class);

    @Autowired
    public NumberOfVehiclesStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util, new LatestAverageVelocityStreamBuilder.KeySerde(), new ValueSerde());
    }


    @Override
    protected KStream<Triplet<Integer, Integer, Boolean>, Pair<Long, Integer>> getStream(KStreamBuilder builder) {
        logger.debug("Building stream to identify number of vehicles at expressway, segment and direction per minute.");

        KStream<Pair<Long, Integer>, Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> source1 =
                builder.stream(PositionReportHandler.TOPIC);

        return
                // map to (expressway, segment, direction) (speed)
                source1.map((k, v) -> new KeyValue<>(new Triplet<>(v.getValue1(), v.getValue4(), v.getValue3()), new Pair<>(v.getValue0(), k.getValue0())))
                        // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                        .aggregateByKey(() -> new Pair<>(0l, 0),
                                (key, value, aggregat) -> {
                                    return new Pair<>(Math.max(aggregat.getValue1(), minuteOfReport(value.getValue1())), aggregat.getValue1() + 1);
                                }, TimeWindows.of("window", 60)).toStream().map((k,v) -> new KeyValue<>(k.key(), v));

    }


    @Override
    protected String getOutputTopic() {
        return TOPIC;
    }

    public static class ValueSerde extends SerdePrototype<Pair<Long, Integer>> {
        public ValueSerde() {
            super(new TupleSerdes.PairSerdes<>());
        }
    }

}
