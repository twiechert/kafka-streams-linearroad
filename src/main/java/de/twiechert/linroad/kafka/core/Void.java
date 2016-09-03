package de.twiechert.linroad.kafka.core;

import de.twiechert.linroad.kafka.core.serde.DefaultSerde;

import java.io.Serializable;

/**
 * Kafka messages always have a key and a value but in some cases you only need either.
 * In that case simply use the Void class for the key/value respectively.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class Void implements Serializable {


    @Override
    public String toString() {
        return "";
    }

    @Override
    public boolean equals(Object obj) {
        return true;
    }

    public static class Serializer extends DefaultSerde.DefaultSerializer<Void> {
    }

}
