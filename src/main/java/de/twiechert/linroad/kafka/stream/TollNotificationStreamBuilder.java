package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
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
 * This stream realizes the toll notification.
 * It is not the same as the current toll stream {@link CurrentTollStreamBuilder}. A couple of things have to be considered here:
 *
 * (I) tolls are triggerd by the position report of a vehicle
 * (II) tolls are only calculated, if a vehicle has changed the segment since the last position report (which is not guaranteed)
 * (III) if the new segment is an exit lane, neither a toll is notified/assessed
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 *
 */
@Component
public class TollNotificationStreamBuilder extends StreamBuilder<Void, TollNotification> {

    public static String TOPIC = "TOLL_NOT";

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(TollNotificationStreamBuilder.class);


    @Autowired
    public TollNotificationStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util);
    }

    public KStream<Void, TollNotification> getStream(KStream<VehicleIdXwayDirection, SegmentCrossing> consecutivePositionReports,
                                                     KStream<XwaySegmentDirection, CurrentToll> currentTollStream) {
        logger.debug("Building stream to notify drivers about accidents");

        /**
         * If the vehicle exits at the exit ramp of a segment, the toll for that segment is not charged. -> thus position reports on exits can be ignored.
         */



        // has to be remapped to be joinable with current toll stream
        // in order to join, we map the time to minutes (tolls are based on the current minute)
        // but we must preserve the actul timestamp, because it has to be emitted
        /**
         * CURRENT TOLL STREAM
         * s:1 =>  m=0 -> toll, m=1 -> toll, m=2 -> toll, 3 -> toll, m=4 -> toll, m=5 -> toll....
         * s:2 =>  m=0 -> toll, m=1 -> toll, m=2 -> toll, 3 -> toll, m=4 -> toll, m=5 -> toll....
         * s:3 =>  m=0 -> toll, m=1 -> toll, m=2 -> toll, 3 -> toll, m=4 -> toll, m=5 -> toll....
         * .....

         * SEGMENT CROSSING STREAM
         * seconds=0 -> crossing                      seconds=90 -> crossing, seconds=120-> crossing -->
         *
         * Before joining, times have to be remapped, because: "the toll reported for the segment being exited is assessed to the vehicle’s account.
         * Thus, a toll calculation for one segment often is concurrent with an account being debited for the previous segment."
         *
         * 0 -> toll, 30 -> toll, 60 -> toll, 90 -> toll, 120 -> toll, 150 -> toll....
         * 0 -> crossing                      90 -> crossing, 120-> crossing -->
         *
         */
        return consecutivePositionReports.filter((k, v) -> v.getLane() != 4).map((k, v) -> new KeyValue<>(new XwaySegmentDirection(k.getXway(), v.getSegment() - 1, k.getDir()),
                // for joining purpose we need the minute of the position report, but we need to keep the exact timestamp for emiting
                new ConsecutivePosReportIntermediate(Util.minuteOfReport(v.getPredecessorTime()), v.getTime(), k.getVehicleId())))
                // join with current toll stream, create VID, time, current time, speed , toll
                .through(new DefaultSerde<>(), new DefaultSerde<>(), "CONS_POS")
                .join(currentTollStream, (psRep, currToll) -> new TollNotification(psRep.getVehicleId(), psRep.getTime(), LinearRoadKafkaBenchmarkApplication.Context.getCurrentRuntimeInSeconds(), currToll.getVelocity(), currToll.getToll()),
                        JoinWindows.of("POS-TOLLN_WINDOW").with(3), new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>()).map((k, v) -> new KeyValue<>(new Void(), v.setXway(k.getXway())))
                .filter((k, v) -> v.getToll() > 0);

    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }


    public static class ConsecutivePosReportIntermediate extends Triplet<Long, Long, Integer> {

        public ConsecutivePosReportIntermediate(Long timeMinute, Long time, Integer vehicleId) {
            super(timeMinute, time, vehicleId);
        }

        public Long getTime() {
            return this.getValue1();
        }

        public Integer getVehicleId() {
            return this.getValue2();
        }

    }




}
