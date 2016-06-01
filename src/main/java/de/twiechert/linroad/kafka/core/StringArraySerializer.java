package de.twiechert.linroad.kafka.core;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Created by tafyun on 01.06.16.
 */
public  class StringArraySerializer implements Serializer<StrTuple> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, StrTuple strings) {
        return strings.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}