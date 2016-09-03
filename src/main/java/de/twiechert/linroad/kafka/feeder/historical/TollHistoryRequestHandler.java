package de.twiechert.linroad.kafka.feeder.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.feeder.TupleHandler;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleDay;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.pDob;
import static de.twiechert.linroad.kafka.core.Util.pInt;

/**
 * This class handles a toll history tuple by sending it to the respective Kafka topic.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class TollHistoryRequestHandler extends TupleHandler<XwayVehicleDay, Double> {

    public static final String TOPIC = "TOLL_HIST_TABLE";

    @Autowired
    public TollHistoryRequestHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    @Override
    protected XwayVehicleDay transformKey(String[] tuple) {
        return new XwayVehicleDay(pInt(tuple[2]), pInt(tuple[0]), pInt(tuple[1]));
    }

    @Override
    protected Double transformValue(String[] tuple) {
        return pDob(tuple[3]);
    }

    @Override
    protected Class<? extends Serializer<XwayVehicleDay>> getKeySerializerClass() {
        return XwayVehicleDay.Serializer.class;
    }

    @Override
    protected Class<? extends Serializer<Double>> getValueSerializerClass() {
        return DoubleSerializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }
}
