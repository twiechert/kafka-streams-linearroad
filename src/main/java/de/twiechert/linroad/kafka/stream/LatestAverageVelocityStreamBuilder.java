package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;

/**
 * This class builds the stream of latest average velocities keyed by (expressway, segment, direction).
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class LatestAverageVelocityStreamBuilder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(LatestAverageVelocityStreamBuilder.class);


    public KStream<XwaySegmentDirection, AverageVelocity> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify latest average velocity");

        return positionReportStream.mapValues(v ->new Pair<>(v.getSpeed(), v.getTime()))
                        // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                .aggregateByKey(() -> new LatestAverageVelocityIntermediate(0, 0d, 0L),
                                (key, value, aggregat) -> {
                                    int n = aggregat.getValue0() + 1;
                                    return new LatestAverageVelocityIntermediate(n, aggregat.getValue1() * (((double) n - 1) / n) + (double) value.getValue0() / n, Math.max(aggregat.getValue2(), minuteOfReport(value.getValue1())));
                                }, TimeWindows.of("LAV_WINDOW", 5 * 60).advanceBy(60), new DefaultSerde<>(), new DefaultSerde<>())
                        .toStream().map((k, v) -> new KeyValue<>(k.key(), new AverageVelocity(v.getValue2(), v.getValue1())));


    }


    public static class LatestAverageVelocityIntermediate extends Triplet<Integer, Double, Long> {
        public LatestAverageVelocityIntermediate(Integer value0, Double value1, Long value2) {
            super(value0, value1, value2);
        }
    }

}
