package de.twiechert.linroad.kafka.core;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Created by tafyun on 01.06.16.
 */


public  class StringArrayDeserializer implements Deserializer<StrTuple> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public StrTuple deserialize(String s, byte[] bytes) {
        return new StrTuple(new String(bytes, StandardCharsets.UTF_8).split(","));
    }

    @Override
    public void close() {

    }
}
