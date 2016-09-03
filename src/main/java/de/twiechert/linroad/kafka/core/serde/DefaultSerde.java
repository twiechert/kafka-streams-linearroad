package de.twiechert.linroad.kafka.core.serde;

import de.twiechert.linroad.kafka.core.serde.provider.FSTSerde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;

/**
 * This class extends the currently used default Serde.
 * When you change the super-class, Kafka will use that implementation.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class DefaultSerde<T extends Serializable> extends FSTSerde<T> {


    public static class DefaultSerializer<A> extends FSTSerde.FSTSerializer<A> implements Serializer<A> {

    }


}
