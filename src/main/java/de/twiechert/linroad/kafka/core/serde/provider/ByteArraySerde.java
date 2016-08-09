package de.twiechert.linroad.kafka.core.serde.provider;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by tafyun on 10.07.16.
 */
@Deprecated
public class ByteArraySerde<T extends Serializable> implements Serde<T> {


    @Deprecated
    public static class BArraySerializer<A> implements Serializer<A> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String s, A a) {
           return SerializationUtils.serialize(a);
        }

        @Override
        public void close() {

        }
    }

    @Deprecated
    public static class BArrayDeserializer<A> implements Deserializer<A> {


        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public A deserialize(String s, byte[] bytes)
        {
                return (A) SerializationUtils.deserialize(bytes);


        }

        @Override
        public void close() {

        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new BArraySerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new BArrayDeserializer<>();
    }
}
