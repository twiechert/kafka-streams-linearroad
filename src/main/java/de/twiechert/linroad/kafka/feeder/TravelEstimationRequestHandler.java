package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.apache.kafka.common.serialization.Serializer;
import org.javatuples.Octet;
import org.javatuples.Quintet;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 22.07.16.
* key corresponds to (Time: t, VID: v, QID: q, XWay: x, Sinit: i, Send: e, DOW: d, TOD: y)
 */

public class TravelEstimationRequestHandler extends TupleHandler<Octet<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Void> {

    public static final String TOPIC = "TRAVELEST";


    public TravelEstimationRequestHandler() {
        super(4);
    }

    @Override
    protected Octet<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> transformKey(String[] tuple) {
        return new Octet<>(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[3]), pInt(tuple[4]), pInt(tuple[5]), pInt(tuple[6]), pInt(tuple[7]), pInt(tuple[8]) );
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<Octet<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>> getKeySerializerClass() {
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

    public static class KeySerializer extends ByteArraySerde.BArraySerializer<Octet<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {}

}
