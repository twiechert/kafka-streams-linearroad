package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import de.twiechert.linroad.kafka.stream.processor.OnMinuteChangeEmitter;
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

        KStream<XwaySegmentDirection, AverageVelocity> lavAgg = positionReportStream.mapValues(v -> new Pair<>(v.getTime(), v.getSpeed()))
                .aggregateByKey(() -> new LatestAverageVelocityIntermediate(0L, 0, 0d),
                        (key, value, agg) -> LatestAverageVelocityIntermediate.fromLast(agg, value.getValue0(), value.getValue1())
                        , lavWindow, new DefaultSerde<>(), new DefaultSerde<>())
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), new AverageVelocity(Util.minuteOfReport(k.window().end()), v.getValue2(), v.getPosReportMinute())));

        return OnMinuteChangeEmitter.getForWindowed(context.getBuilder(), lavAgg, new DefaultSerde<>(), new DefaultSerde<>(), "latest-lav");


    }

    public static class LatestAverageVelocityIntermediate extends Triplet<Long, Integer, Double> {

        public static LatestAverageVelocityIntermediate fromLast(LatestAverageVelocityIntermediate last, long timestamp, int velocity) {
            int n = last.getNumberOfReports() + 1;
            return new LatestAverageVelocityIntermediate(Util.minuteOfReport(timestamp),
                    n, last.getAverageVelocity() * (((double) n - 1) / n) + (double) velocity / n);
        }

        public LatestAverageVelocityIntermediate(Long posReportMinute, Integer numberOfReports, Double averageVelocity) {
            super(posReportMinute, numberOfReports, averageVelocity);
        }

        public double getAverageVelocity() {
            return getValue2();
        }

        public int getNumberOfReports() {
            return getValue1();
        }

        public long getPosReportMinute() {
            return this.getValue0();
        }


    }
}
