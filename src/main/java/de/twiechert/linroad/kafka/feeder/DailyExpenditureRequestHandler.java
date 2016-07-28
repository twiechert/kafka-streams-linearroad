package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.apache.kafka.common.serialization.Serializer;
import org.javatuples.Quintet;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 21.07.16.
 *
 * Key corresponds to (Time: t, VID: v, QID: q, XWay: x, Day: n).
 */
public class DailyExpenditureRequestHandler extends TupleHandler<Quintet<Long, Integer, Integer, Integer, Integer>, Void> {

    public static final String TOPIC = "DAILYEXP";

    public DailyExpenditureRequestHandler() {
        super(3);
    }

    @Override
    protected Quintet<Long, Integer, Integer, Integer, Integer> transformKey(String[] tuple) {
        return new Quintet<>(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[3]), pInt(tuple[4]), pInt(tuple[5]));
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<Quintet<Long, Integer, Integer, Integer, Integer>>> getKeySerializerClass() {
        return KeySerializer.class;
    }

    @Override
    protected Class<? extends Serializer<Void>> getValueSerializerClass() {
        return AccountBalanceRequestHandler.ValueSerializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }

    public static class KeySerializer extends ByteArraySerde.BArraySerializer<Quintet<Long, Integer, Integer, Integer, Integer>> {}

}
