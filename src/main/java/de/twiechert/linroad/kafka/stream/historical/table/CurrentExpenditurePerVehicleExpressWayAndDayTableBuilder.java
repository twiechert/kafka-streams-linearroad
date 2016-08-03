package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.model.TollNotification;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleDay;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.javatuples.Pair;
import org.springframework.stereotype.Component;


/**
 * This class transforms the toll notification stream to a table that holds the current cumulated expenditures per vehicle, xway and day.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class CurrentExpenditurePerVehicleExpressWayAndDayTableBuilder {


    public CurrentExpenditurePerVehicleExpressWayAndDayTableBuilder() {
    }

    public KTable<XwayVehicleDay, Double> getStream(KStream<Void, TollNotification> tollNotification) {
        return tollNotification.map((k, v) -> new KeyValue<>(new XwayVehicleDay(v.getXway(), v.getVehicleId(), Util.dayOfReport(v.getResponseTime())), v)
        ).aggregateByKey(() -> 0d, (key, value, aggregat) -> aggregat + value.getToll(), "CURR_TOLL_PER_XWAY_VEH_DAY_WINDOW")
                .through(new XwayVehicleDay.Serde(), new Serdes.DoubleSerde(), "CURR_TOLL_PER_XWAY_VEH_DAY");

    }
}
