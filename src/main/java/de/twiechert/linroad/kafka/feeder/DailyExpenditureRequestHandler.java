package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.model.historical.DailyExpenditureRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 21.07.16.
 *
 * Key corresponds to (Time: t, VID: v, QID: q, XWay: x, Day: n).
 */
@Component
public class DailyExpenditureRequestHandler extends TupleHandler<DailyExpenditureRequest, Void> {

    public static final String TOPIC = "DAILYEXP";

    @Autowired
    public DailyExpenditureRequestHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context, 3);
    }

    @Override
    protected DailyExpenditureRequest transformKey(String[] tuple) {
        return new DailyExpenditureRequest(pLng(tuple[1]), pInt(tuple[2]), pInt(tuple[3]), pInt(tuple[4]), pInt(tuple[5]));
    }

    @Override
    protected Void transformValue(String[] tuple) {
        return new Void();
    }

    @Override
    protected Class<? extends Serializer<DailyExpenditureRequest>> getKeySerializerClass() {
        return DailyExpenditureRequest.Serializer.class;
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
