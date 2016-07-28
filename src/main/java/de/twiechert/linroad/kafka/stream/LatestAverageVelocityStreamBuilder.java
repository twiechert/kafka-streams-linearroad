package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.TupleTimestampExtrator;
import de.twiechert.linroad.kafka.core.serde.SerdePrototype;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;

/**
 * This class builds the stream of latest average velocities keyed by (expressway, segment, direction).
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class LatestAverageVelocityStreamBuilder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(LatestAverageVelocityStreamBuilder.class);

    public LatestAverageVelocityStreamBuilder() {
    }


    public KStream<XwaySegmentDirection, AverageVelocity> getStream(KStream<PositionReport.Key, PositionReport.Value> positionReportStream) {
        logger.debug("Building stream to identify latest average velocity");

        return
                // map to (expressway, segment, direction) -> (speed)
                positionReportStream.map((k, v) -> new KeyValue<>(new XwaySegmentDirection(v.getXway(), v.getSeg(), v.getDir()), new Pair<>(v.getSpeed(), k.getTime())))
                        // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                        .aggregateByKey(() -> new Triplet<>(0, 0d, 0l),
                                (key, value, aggregat) -> {
                                    int n = aggregat.getValue0() + 1;
                                    return new Triplet<>(n, aggregat.getValue1() * (((double) n - 1) / n) + (double) value.getValue0() / n, Math.max(aggregat.getValue2(), minuteOfReport(value.getValue1()) + 1));
                                }, TimeWindows.of("LAV_WINDOW", 5 * 60).advanceBy(60), new XwaySegmentDirection.Serde(), new TupleSerdes.TripletSerdes<>())
                        .toStream().map((k, v) -> new KeyValue<>(k.key(), new AverageVelocity(v.getValue2(), v.getValue1())));


    }


    /**
     * This timestamp extractor can be used for stream generated by @link{LatestAverageVelocityTableAndStreamBuilder} but also for that generated by
     *
     * @link{NumberOfVehiclesStreamBuilder}
     */
    public static class TimeStampExtractor extends TupleTimestampExtrator implements TimestampExtractor {
        public TimeStampExtractor() {
            super(KeyValue.Value, 0);
        }
    }

    public static class KeySerde extends SerdePrototype<Triplet<Integer, Integer, Boolean>> {
        public KeySerde() {
            super(new TupleSerdes.TripletSerdes<>());
        }
    }


}
