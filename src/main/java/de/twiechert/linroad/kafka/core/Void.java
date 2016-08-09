package de.twiechert.linroad.kafka.core;

import de.twiechert.linroad.kafka.core.serde.DefaultSerde;

import java.io.Serializable;

/**
 * Created by tafyun on 21.07.16.
 */
public class Void implements Serializable {


    @Override
    public String toString() {
        return "";
    }


    public static class Serializer extends DefaultSerde.DefaultSerializer<Void> {
    }

}
