package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import de.twiechert.linroad.kafka.stream.windowing.LavWindow;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class builds the stream of latest average velocities keyed by (expressway, segment, direction).
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class LatestAverageVelocityStreamBuilder {


    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(LatestAverageVelocityStreamBuilder.class);


    public KStream<XwaySegmentDirection, AverageVelocity> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify latest average velocity");
        LavWindow lavWindow = LavWindow.of(context.topic("LAV_WINDOW"));

        return positionReportStream.mapValues(v -> new Pair<>(v.getSpeed(), v.getTime()))
                .aggregateByKey(() -> new LatestAverageVelocityIntermediate(0L, 0, 0d),
                        (key, value, agg) -> {
                            int n = agg.getValue1() + 1;
                            return new LatestAverageVelocityIntermediate(value.getValue1(), n, agg.getValue2() * (((double) n - 1) / n) + (double) value.getValue0() / n);
                        }, lavWindow, new DefaultSerde<>(), new DefaultSerde<>())
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), new AverageVelocity(Util.minuteOfReport(k.window().end()), v.getValue2())));


    }

    public static class LatestAverageVelocityIntermediate extends Triplet<Long, Integer, Double> {
        public LatestAverageVelocityIntermediate(Long time, Integer value0, Double value1) {
            super(time, value0, value1);
        }
    }
}
