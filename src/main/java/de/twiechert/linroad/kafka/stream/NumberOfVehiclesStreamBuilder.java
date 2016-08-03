package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.model.NumberOfVehicles;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;


/**
 * This class builds the number of vehicles that per (expressway, segment, direction) tuple.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class NumberOfVehiclesStreamBuilder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(NumberOfVehiclesStreamBuilder.class);

    public NumberOfVehiclesStreamBuilder() {
    }


    public KStream<XwaySegmentDirection, NumberOfVehicles> getStream(KStream<XwaySegmentDirection, PositionReport.Value> positionReportStream) {
        logger.debug("Building stream to identify number of vehicles at expressway, segment and direction per minute.");

        return positionReportStream.mapValues(v -> new Pair<>(v.getSpeed(), v.getTime()))
                // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                .aggregateByKey(() -> new NumberOfVehicles(0l, 0),
                        (key, value, aggregat) -> {
                            //   return new NumberOfVehicles(Math.max(aggregat.getValue1(), minuteOfReport(value.getValue1())), aggregat.getValue1() + 1);
                            return new NumberOfVehicles(minuteOfReport(value.getValue1()), aggregat.getValue1() + 1);

                        }, TimeWindows.of("NOV-WINDOW", 5), new XwaySegmentDirection.Serde(), new NumberOfVehicles.Serde()).toStream().map((k, v) -> new KeyValue<>(k.key(), v))
                .filter((k, v) -> v.getNumber() > 40);

    }


}
