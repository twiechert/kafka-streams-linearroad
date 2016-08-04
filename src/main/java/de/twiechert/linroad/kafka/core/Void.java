package de.twiechert.linroad.kafka.core;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;

import java.io.Serializable;

/**
 * Created by tafyun on 21.07.16.
 */
public class Void implements Serializable {

    @Override
    public String toString() {
        return "";
    }

    public static class Serde extends ByteArraySerde<Void> {
    }

    public static class Serializer extends ByteArraySerde.BArraySerializer<Void> {
    }

}
