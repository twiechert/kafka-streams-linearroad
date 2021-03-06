package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.AccountBalanceRequestHandler;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * This class reads the tuples from the account balance stream and returns the respective stream object for further usage.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class AccountBalanceRequestStreamBuilder {

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    public AccountBalanceRequestStreamBuilder() {
    }

    public KStream<AccountBalanceRequest, Void> getStream(KStreamBuilder builder) {
        return builder.stream(new AccountBalanceRequest.Serde(),
                new DefaultSerde<>(), context.topic(AccountBalanceRequestHandler.TOPIC));
    }
}
