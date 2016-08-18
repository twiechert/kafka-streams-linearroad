package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 15.08.16.
 */
@Component
public class MinuteTimer extends TupleHandler<Void, Void> {


    @Autowired
    public MinuteTimer(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    @Override
    protected Void transformKey(String[] tuple) {
        return new Void();
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<Void>> getKeySerializerClass() {
        return Void.Serializer.class;
    }

    @Override
    protected Class<? extends Serializer<Void>> getValueSerializerClass() {
        return Void.Serializer.class;
    }

    @Override
    protected String getTopic() {
        return "MinuteTimer";
    }
}
