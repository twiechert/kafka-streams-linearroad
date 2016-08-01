package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.model.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 29.07.16.
 */
@Component
public class TollNotificationStreamBuilder extends StreamBuilder<Void, TollNotification> {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(TollNotificationStreamBuilder.class);

    @Autowired
    public TollNotificationStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util, new Void.Serde(), new TollNotification.Serde());
    }

    public KStream<Void, TollNotification> getStream(KStream<XwaySegmentDirection, PositionReport.Value> positionReports,
                                                     KStream<XwaySegmentDirection, CurrentToll> currentTollStream) {
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
        KStream<VehicleIdXwayDirection, Pair<Long, Integer>> consecutivePositionReports = filteredPositionReports.join(filteredPositionReports, (report1, report2) -> new Triplet<>(report1.getValue0(), report1.getValue1(), report1.getValue1().equals(report2.getValue1())),
                JoinWindows.of("POS-POS-WINDOW").after(30), new VehicleIdXwayDirection.Serde(), new TupleSerdes.PairSerdes<>(), new TupleSerdes.PairSerdes<>())
                .filter((k, v) -> v.getValue2()).mapValues(v -> new Pair<>(v.getValue0(), v.getValue1()));

        // has to be remapped to be joinable with current toll stream
        return consecutivePositionReports.map((k, v) -> new KeyValue<>(new XwaySegmentDirection(k.getXway(), v.getValue1(), k.getDir()), new Pair<>(Util.minuteOfReport(v.getValue0()), k.getVehicleId())))
                // join with current toll stream, create VID, time, current time, speed , toll
                .through(new XwaySegmentDirection.Serde(), new TupleSerdes.PairSerdes<>(), "CONS_POS")
                .join(currentTollStream, (psRep, currentToll) -> new TollNotification(psRep.getValue1(), psRep.getValue0(), psRep.getValue0(), currentToll.getVelocity(), currentToll.getToll()),
                        JoinWindows.of("POS-TOLLN_WINDOW")).map((k, v) -> new KeyValue<>(new Void(), v));


    }

    @Override
    public String getOutputTopic() {
        return "toll-notification";
    }
}
