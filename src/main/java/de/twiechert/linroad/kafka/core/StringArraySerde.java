package de.twiechert.linroad.kafka.core;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by tafyun on 31.05.16.
 */
public class StringArraySerde implements Serde<StrTuple> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<StrTuple> serializer() {
        return new StringArraySerializer();
    }

    @Override
    public Deserializer<StrTuple> deserializer() {
        return new StringArrayDeserializer();
    }
}
