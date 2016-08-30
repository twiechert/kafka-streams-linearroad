package de.twiechert.linroad.kafka.stream.historical.table;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.historical.TollHistoryRequestHandler;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleDay;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * This class transforms the toll notification stream to a table that holds the current cumulated expenditures per vehicle, xway and day.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class TollHistoryTableBuilder {


    public static final String TOPIC = "TOLL_HISTORY";
    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public TollHistoryTableBuilder() {
    }

    public KTable<XwayVehicleDay, Double> getTable(KStreamBuilder builder) {
        return builder.table(new XwayVehicleDay.Serde(),
                new Serdes.DoubleSerde(), context.topic(TollHistoryRequestHandler.TOPIC))
                /**
                 * Table gets fixed name
                 */
                .through(new DefaultSerde<>(), new Serdes.DoubleSerde(), TOPIC);

    }

    public KTable<XwayVehicleDay, Double> getExistingTable(KStreamBuilder builder) {

        return builder.table(new XwayVehicleDay.Serde(),
                new Serdes.DoubleSerde(), TOPIC);

    }
}
