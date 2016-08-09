package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.TollNotification;
import de.twiechert.linroad.kafka.model.historical.ExpenditureAt;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;


/**
 * This class transforms the toll notification stream to a table that holds the current cumulated expenditures per vehicle.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class CurrentExpenditurePerVehicleTableBuilder {

    public KTable<Integer, ExpenditureAt> getStream(KStream<Void, TollNotification> tollNotification) {

        // re-key be vehicle id
        return tollNotification.map((k, v) -> new KeyValue<>(v.getVehicleId(), v))
                .aggregateByKey(() -> new ExpenditureAt(0L, 0d), (key, value, aggregat) -> {

                            return new ExpenditureAt(Math.max(aggregat.getTime(), value.getRequestTime()), aggregat.getExpenditure() + value.getToll());
                        }
                        , new Serdes.IntegerSerde(), new DefaultSerde<>(), "CURR_TOLL_PER_VEH_WINDOW")
                .through(new Serdes.IntegerSerde(), new DefaultSerde<>(), "CURR_TOLL_PER_VEH");

    }
}
