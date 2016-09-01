package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import de.twiechert.linroad.kafka.stream.processor.ComparableSlidingWindowWrapper;
import de.twiechert.linroad.kafka.stream.processor.Punctuator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;

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
        TimeWindows lavWindow = TimeWindows.of(context.topic("LAV_WINDOW"), 5 * 60).advanceBy(60);

        return positionReportStream.mapValues(v -> new Pair<>(v.getSpeed(), v.getTime()))
                .aggregateByKey(() -> new LatestAverageVelocityIntermediate(0l, 0, 0d),
                        (key, value, agg) -> {
                            int n = agg.getValue1() + 1;
                            return new LatestAverageVelocityIntermediate(value.getValue1(), n, agg.getValue2() * (((double) n - 1) / n) + (double) value.getValue0() / n);
                        }, lavWindow, new DefaultSerde<>(), new DefaultSerde<>())
                .toStream()
                .flatMap((k, v) -> {
                    List<KeyValue<XwaySegmentDirection, AverageVelocity>> elements = new ArrayList<>();
                    // special treatment for the first elements
                    if (k.window().end() == 300l) {
                        for (long i = (300); i >= v.getValue0(); i -= 60) {
                            elements.add(new KeyValue<>(k.key(), new AverageVelocity(Util.minuteOfReport(i), v.getValue2())));


                        }
                    } else {
                        elements.add(new KeyValue<>(k.key(), new AverageVelocity(Util.minuteOfReport(k.window().end()), v.getValue2())));
                    }
                    return elements;
                });

    }

    public static class LatestAverageVelocityIntermediate extends Triplet<Long, Integer, Double> {
        public LatestAverageVelocityIntermediate(Long time, Integer value0, Double value1) {
            super(time, value0, value1);
        }
    }
}
