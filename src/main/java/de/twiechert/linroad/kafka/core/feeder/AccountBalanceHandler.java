package de.twiechert.linroad.kafka.core.feeder;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.apache.kafka.common.serialization.Serializer;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.javatuples.Triplet;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 21.07.16.
 */
public class AccountBalanceHandler extends TupleHandler<Triplet<Long, Integer, Integer>, Void> {

    public static final String TOPIC = "BALANCE";


    public AccountBalanceHandler() {
        super(2);
    }

    @Override
    protected Triplet<Long, Integer, Integer> transformKey(String[] tuple) {
        return new Triplet<>(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[3]));
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<Triplet<Long, Integer, Integer>>> getKeySerializerClass() {
        return KeySerializer.class;
    }

    @Override
    protected Class<? extends Serializer<Void>> getValueSerializerClass() {
        return ValueSerializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }

    public static class KeySerializer extends ByteArraySerde.BArraySerializer<Triplet<Long, Integer, Integer>> {}

    public static class ValueSerializer extends ByteArraySerde.BArraySerializer<Void> {}
}
