package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.VehicleIdXwayDirection;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 29.07.16.
 */
@Component
public class TollNotificationStreamBuilder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(TollNotificationStreamBuilder.class);


    public KStream<VehicleIdXwayDirection, Triplet<Long, Integer, Boolean>> getStream(KStream<XwaySegmentDirection, PositionReport.Value> positionReports,
                                                                                      KStream<XwaySegmentDirection, Pair<Double, Double>> currentTollStream) {
        logger.debug("Building stream to notify drivers about accidents");

        /**
         * If the vehicle exits at the exit ramp of a segment, the toll for that segment is not charged. -> thus position reports on exits can be ignored.
         * We consider position reports per xway, segment, drection and vehicle -> thus remapping
         */
        KStream<VehicleIdXwayDirection, Pair<Long, Integer>> filteredPositionReports = positionReports.filter((k, v) -> v.getLane() != 4)
                .map((k, v) -> new KeyValue<>(new VehicleIdXwayDirection(v.getVehicleId(), k), new Pair<>(v.getTime(), k.getSeg())))
                .through(new VehicleIdXwayDirection.Serde(), new TupleSerdes.PairSerdes<>(), "POS_BY_VEHICLE");


        /**
         * ... must calculate a toll every time a vehicle reports a position in a new segment, and notify the driver of this toll.
         * --> we must check if the segment has changed since the last psotion report of that vehicle.
         * Because Kafka streams does not support data-driven windows, we self-join the position report with a slide of 30 seconds
         */
        // consider that a position report should only
        KStream<VehicleIdXwayDirection, Triplet<Long, Integer, Boolean>> consecutivePositionReports = filteredPositionReports.join(filteredPositionReports, (report1, report2) -> new Triplet<>(report1.getValue0(), report1.getValue1(), report1.getValue1().equals(report2.getValue1())),
                JoinWindows.of("POS-POS-WINDOW").after(30), new VehicleIdXwayDirection.Serde(), new TupleSerdes.PairSerdes<>(), new TupleSerdes.PairSerdes<>());

        return consecutivePositionReports;

        /*

        consecutivePositionReports

                        .join(currentTollStream, (value1, value2) -> new Triplet<>(value1.getTime(), value1.getVehicleId(), value2), JoinWindows.of("POS-TOLL-WINDOW").before(1),
                                new XwaySegmentDirection.Serde(), new PositionReport.ValueSerde(), new Serdes.DoubleSerde())
                        .map((k, v) -> new KeyValue<>(new Quartet<>()));


        // IMPORTANT for joining: , but only if q (position report) was emitted
        // **no** earlier than the minute following them inutew hen the accident occurred.
        // i.e. the accident detection must be "before" up to one second
        return  accidentReports.through(new XwaySegmentDirection.Serde(), new Serdes.LongSerde(), "ACC_DET_NOT")
                .join(positionReports, (value1, value2) -> new Pair<>(value1, value1), JoinWindows.of("ACC-NOT-WINDOW").before(1),
                        new XwaySegmentDirection.Serde(), new Serdes.LongSerde(), new PositionReport.ValueSerde())
                .map((k, v) -> new KeyValue<>(new Void(), new Quartet<>(1, v.getValue0(), v.getValue1(), k.getValue1())));
                */

    }
}
