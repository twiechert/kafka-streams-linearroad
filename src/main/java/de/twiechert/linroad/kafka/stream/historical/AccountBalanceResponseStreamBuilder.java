package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceResponse;
import de.twiechert.linroad.kafka.model.historical.ExpenditureAt;
import de.twiechert.linroad.kafka.stream.StreamBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This stream represents the response of account balance requests. In order to respond to these requests,
 * the actual request stream is joined with a table of expenditures per vehicle.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class AccountBalanceResponseStreamBuilder extends StreamBuilder<Void, AccountBalanceResponse> {

    public final static String TOPIC = "ACCOUNT_BALANCE_RESP";

    @Autowired
    public AccountBalanceResponseStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }

    public KStream<Void, AccountBalanceResponse> getStream(KStream<AccountBalanceRequest, Void> accountBalanceRequestStream,
                                                           KTable<Integer, ExpenditureAt> currentTollTable) {
        /**
         * Change the key of the request stream such that we can aggregate on a per vehicle basis.
         */
        KStream<Integer, AccountBalanceRequest> accountBalanceRequestsPerVehicle = accountBalanceRequestStream.map((k, v) -> new KeyValue<>(k.getVehicleID(), k))
                .through(new Serdes.IntegerSerde(), new DefaultSerde<>(), context.topic("ACC_BALANCE_PER_VEHICLE"));


        return accountBalanceRequestsPerVehicle.leftJoin(currentTollTable,
                (accValue, tollVal) -> new AccountBalanceResponse(accValue.getRequestTime(),
                        LinearRoadKafkaBenchmarkApplication.Context.getCurrentRuntimeInSeconds(),
                        (tollVal == null) ?
                                -1 :
                                tollVal.getValue0(),
                        accValue.getQueryId(),
                        (tollVal == null) ?
                                0 :
                                tollVal.getValue1()))
                .selectKey((k, v) -> new Void());

    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }
}
