package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.NumberOfVehicles;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashSet;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;


/**
 * This class builds the number of vehicles that per (expressway, segment, direction) tuple.
 * The tuples that are output corresspond to minute m+1...
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class NumberOfVehiclesStreamBuilder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(NumberOfVehiclesStreamBuilder.class);


    public KStream<XwaySegmentDirection, NumberOfVehicles> getStream(KStream<XwaySegmentDirection, PositionReport> positionReportStream) {
        logger.debug("Building stream to identify number of vehicles at expressway, segment and direction per minute.");

        return positionReportStream.mapValues(v -> new Pair<>(v.getVehicleId(), v.getTime()))
                // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                .aggregateByKey(() -> new Pair<>(0L, new HashSet<>()), (key, value, agg) -> {
                    //  aggregat.getValue1() + 1
                    agg.getValue1().add(value.getValue0());
                    return new Pair<>(minuteOfReport(value.getValue1() + 1), agg.getValue1());

                }, TimeWindows.of("NOV-WINDOW", 60), new DefaultSerde<>(), new DefaultSerde<>())
                .toStream().map((k, v) -> new KeyValue<>(k.key(), new NumberOfVehicles(v.getValue0(), v.getValue1().size())));

    }


    public static class VehicleIdTimeIntermediate extends Pair<Long, HashSet<Integer>> {
        public VehicleIdTimeIntermediate(Long value0, HashSet<Integer> value1) {
            super(value0, value1);
        }

    }


}
