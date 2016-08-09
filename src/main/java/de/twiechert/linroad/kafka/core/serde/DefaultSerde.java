package de.twiechert.linroad.kafka.core.serde;

import de.twiechert.linroad.kafka.core.serde.provider.FSTSerde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;

/**
 * Created by tafyun on 04.08.16.
 */
public class DefaultSerde<T extends Serializable> extends FSTSerde<T> {


    public static class DefaultSerializer<A> extends FSTSerde.FSTSerializer<A> implements Serializer<A> {

    }


}
