package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.SegmentCrossing;
import de.twiechert.linroad.kafka.model.TollNotification;
import de.twiechert.linroad.kafka.model.VehicleIdXwayDirection;
import de.twiechert.linroad.kafka.model.historical.ExpenditureAt;
import de.twiechert.linroad.kafka.stream.SegmentCrossingPositionReportBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;


/**
 * This class transforms the toll notification stream to a table that holds the current cumulated expenditures per vehicle.
 * Note:
 * "Every time a vehicle issues its first position report from a seg- ment, a toll for that segment is calculated and the vehicle is notified of that toll.
 * Every time a position report identifies a vehicle as crossing from one segment into another,
 * the toll reported for the segment being exited is assessed to the vehicle’s account.
 * Thus, a toll calculation for one seg- ment often is concurrent with an account being debited for the previous segment."
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class CurrentExpenditurePerVehicleTableBuilder {

    public KTable<Integer, ExpenditureAt> getStream(KStream<VehicleIdXwayDirection, SegmentCrossing> consecutivePositionReports,
                                                    KStream<Void, TollNotification> tollNotification) {


        // remap segment crossing position reports to vehicleid only -> when has vehicle changed segment
        KStream<Integer, Long> crossingPositionReportByVehicle = consecutivePositionReports.map((k, v) -> new KeyValue<>(k.getVehicleId(), v.getValue0()))
                .through(new DefaultSerde<>(), new DefaultSerde<>(), "CROSS_POS_REPORT_PER_VEHICLE");

        // re-key be vehicle id
        return tollNotification.map((k, v) -> new KeyValue<>(v.getVehicleId(), new org.javatuples.Pair<>(v.getRequestTime(), v.getToll())))
                .through(new DefaultSerde<>(), new DefaultSerde<>(), "TOLL_NOT_PER_VEHICLE")
                .join(crossingPositionReportByVehicle, (tollnot, cross) -> tollnot, JoinWindows.of("CROSS_POS_TOLL_WINDOW")
                        , new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>())
                .aggregateByKey(() -> new ExpenditureAt(0L, 0d), (key, value, aggregat) -> {

                            return new ExpenditureAt(Math.max(aggregat.getTime(), value.getValue0()), aggregat.getExpenditure() + value.getValue1());
                        }
                        , new Serdes.IntegerSerde(), new DefaultSerde<>(), "CURR_TOLL_PER_VEH_WINDOW")
                .through(new Serdes.IntegerSerde(), new DefaultSerde<>(), "CURR_TOLL_PER_VEH");

    }
}
