package de.twiechert.linroad.kafka.core.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.Serializable;

/**
 * Created by tafyun on 04.08.16.
 */
public class DefaultSerde<T extends Serializable> extends JacksonSmileSerde<T> {

    public DefaultSerde(Class<T> classOb) {
        super(classOb);
    }

    /*
        public static class DefaultSerializer<A>  extends ByteArraySerde.BArraySerializer<A> implements Serializer<A> {

        }

        public static class DefaultDeserializer<A>  extends ByteArraySerde.BArrayDeserializer<A> implements Deserializer<A> {

        }


       public static class DefaultSerializer<A>  extends KryoSerde.KryoSerializer<A> implements Serializer<A> {

       }

        public static class DefaultDeserializer<A>  extends KryoSerde.KryoDeserializer<A> implements Deserializer<A> {

        }

        */
    public static class DefaultSerializer<A> extends JsonSerializer<A> implements Serializer<A> {
        public DefaultSerializer() {
            super(JacksonSmileSerde.getObjectMapper());
        }
    }

    public static class DefaultDeserializer<A> extends JsonDeserializer<A> implements Deserializer<A> {
        public DefaultDeserializer(Class<A> targetType) {
            super(targetType, JacksonSmileSerde.getObjectMapper());
        }
    }


}
