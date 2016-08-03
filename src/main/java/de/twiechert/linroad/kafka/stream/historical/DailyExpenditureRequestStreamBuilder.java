package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.feeder.DailyExpenditureRequestHandler;
import de.twiechert.linroad.kafka.model.historical.AccountBalanceRequest;
import de.twiechert.linroad.kafka.model.historical.DailyExpenditureRequest;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.stereotype.Component;

/**
 * This class reads the tuples from the daily expenditure request stream and returns the respective stream object for further usage.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class DailyExpenditureRequestStreamBuilder {

    public DailyExpenditureRequestStreamBuilder() {
    }

    public KStream<DailyExpenditureRequest, Void> getStream(KStreamBuilder builder) {
        return builder.stream(new DailyExpenditureRequest.Serde(),
                new Void.Serde(), DailyExpenditureRequestHandler.TOPIC);
    }
}
