package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.model.TollNotification;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.javatuples.Pair;
import org.springframework.stereotype.Component;


/**
 * This class transforms the toll notification stream to a table that holds the current cumulated expenditures per vehicle.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class CurrentExpenditurePerVehicleTableBuilder {


    public CurrentExpenditurePerVehicleTableBuilder() {
    }

    public KTable<Integer, Pair<Long, Double>> getStream(KStream<Void, TollNotification> tollNotification) {

        // re-key be vehicle id
        return tollNotification.map((k, v) -> new KeyValue<>(v.getVehicleId(), v)).aggregateByKey(() -> new Pair<>(0l, 0d), (key, value, aggregat) -> {

            return new Pair<>(Math.max(aggregat.getValue0(), value.getRequestTime()), aggregat.getValue1() + value.getToll());
        }, "CURR_TOLL_PER_VEH_WINDOW")
                .through(new Serdes.IntegerSerde(), new TupleSerdes.PairSerdes<>(), "CURR_TOLL_PER_VEH");

    }
}
