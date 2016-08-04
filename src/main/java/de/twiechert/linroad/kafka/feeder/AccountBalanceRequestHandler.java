package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 21.07.16.
 *
 * Key corresponds to (Time: t, VID: v, QID: q).
 */
@Component
public class AccountBalanceRequestHandler extends TupleHandler<AccountBalanceRequest, Void> {

    public static final String TOPIC = "BALANCE";


    @Autowired
    public AccountBalanceRequestHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context, 2);
    }

    @Override
    protected AccountBalanceRequest transformKey(String[] tuple) {
        return new AccountBalanceRequest(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[3]));
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<AccountBalanceRequest>> getKeySerializerClass() {
        return AccountBalanceRequest.Serializer.class;
    }

    @Override
    protected Class<? extends Serializer<Void>> getValueSerializerClass() {
        return Void.Serializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }


}
