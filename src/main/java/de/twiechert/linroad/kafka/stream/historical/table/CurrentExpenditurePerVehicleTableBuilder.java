package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.stream.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.CurrentToll;
import de.twiechert.linroad.kafka.model.SegmentCrossing;
import de.twiechert.linroad.kafka.model.VehicleIdXwayDirection;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import de.twiechert.linroad.kafka.model.historical.ExpenditureAt;
import de.twiechert.linroad.kafka.stream.TollNotificationStreamBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * This class creates a table that holds the sum of expenditures during the simulation until know.
 * An update of that table is triggered by the segment-crossing
 *
 * "Every time a vehicle issues its first position report from a segment, a toll for that segment is calculated and the vehicle is notified of that toll.
 * Every time a position report identifies a vehicle as crossing from one segment into another,
 * the toll reported for the segment being exited is assessed to the vehicle’s account.
 * Thus, a toll calculation for one seg- ment often is concurrent with an account being debited for the previous segment."
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class CurrentExpenditurePerVehicleTableBuilder {

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public KTable<Integer, ExpenditureAt> getStream(KStream<VehicleIdXwayDirection, SegmentCrossing> consecutivePositionReports,
                                                    KStream<XwaySegmentDirection, CurrentToll> currentTollStream) {


        // instead
        return consecutivePositionReports
                /*
                  When position report for change to segment s, we need to assses segment s-1 and for that we need
                  the toll valid at minute(s-1) -> this is shipped with the consecutivePositionReports stream
                 */
                .map((k, v) -> new KeyValue<>(new XwaySegmentDirection(k.getXway(), v.getSegment() - 1, k.getDir()),
                        new TollNotificationStreamBuilder.ConsecutivePosReportIntermediate(Util.minuteOfReport(v.getPredecessorTime()), v.getTime(), k.getVehicleId())))

                .through(new DefaultSerde<>(), new DefaultSerde<>(), "SEG_CROSSINGS_SEG_SHIFTED")
                .join(currentTollStream, (psRep, currToll) -> new CurrentExpenditureIntermediate(psRep.getVehicleId(), currToll.getToll()),

                        JoinWindows.of("SEG_CROSSINGS_SEG_SHIFTED_CURR_TOLL_WINDOW"), new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>())
                .map((k, v) -> new KeyValue<>(v.getVehicleId(), v.getToll()))
                .aggregateByKey(() -> new ExpenditureAt(0L, 0d),
                        (key, value, agg) -> new ExpenditureAt(LinearRoadKafkaBenchmarkApplication.Context.getCurrentRuntimeInSeconds(), agg.getExpenditure() + value),
                        new Serdes.IntegerSerde(), new DefaultSerde<>(), "CURR_TOLL_PER_VEH_WINDOW")
                .through(new Serdes.IntegerSerde(), new DefaultSerde<>(), "CURR_TOLL_PER_VEH");
    }

    public static class CurrentExpenditureIntermediate extends Pair<Integer, Double> {
        public CurrentExpenditureIntermediate(Integer vehicleId, Double toll) {
            super(vehicleId, toll);
        }

        public int getVehicleId() {
            return getValue0();
        }

        public double getToll() {
            return getValue1();
        }
    }

}
