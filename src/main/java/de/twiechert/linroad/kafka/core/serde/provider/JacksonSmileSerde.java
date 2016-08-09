package de.twiechert.linroad.kafka.core.serde.provider;

/**
 * Created by tafyun on 04.08.16.
 */

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.Serializable;
import java.util.Map;


/**
 * Created by tafyun on 10.07.16.
 */
public class JacksonSmileSerde<T extends Serializable> implements Serde<T> {


    private final java.lang.Class<T> classOb;

    public JacksonSmileSerde(Class<T> classOb) {
        this.classOb = classOb;
    }


    @Override
    public Serializer<T> serializer() {
        return new JsonSerializer<>(getObjectMapper());
    }

    @Override
    public Deserializer<T> deserializer() {
        return new JsonDeserializer<T>(this.classOb, getObjectMapper());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    public static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper(new SmileFactory());
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        return mapper;
    }
}
