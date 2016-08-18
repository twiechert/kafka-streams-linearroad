package de.twiechert.linroad.kafka.core.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class SerdePrototype<SerdeOf> implements Serde<SerdeOf> {

    private final Serde<SerdeOf> prototype;

    public SerdePrototype(Serde<SerdeOf> prototype) {
        this.prototype = prototype;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        prototype.configure(configs, isKey);
    }

    @Override
    public void close() {
        prototype.close();
    }

    @Override
    public Serializer<SerdeOf> serializer() {
        return prototype.serializer();
    }

    @Override
    public Deserializer<SerdeOf> deserializer() {
        return prototype.deserializer();
    }
}

