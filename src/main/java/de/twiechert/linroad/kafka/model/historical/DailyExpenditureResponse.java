package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Quintet;

/**
 * Created by tafyun on 02.08.16.
 */
public class DailyExpenditureResponse extends Quintet<Integer, Long, Long, Integer, Double> {


    public DailyExpenditureResponse() {
    }

    public DailyExpenditureResponse(Long requestTime, Long responseTime, Integer queryId, Double balance) {
        super(3, requestTime, responseTime, queryId, balance);
    }

    @JsonIgnore
    public long getRequestTime() {
        return getValue1();
    }

    @JsonIgnore
    public long getResponseTime() {
        return getValue2();
    }

    @JsonIgnore
    public int getQueryId() {
        return getValue3();
    }

    @JsonIgnore
    public double getBalance() {
        return getValue4();
    }


    public static class Serde extends DefaultSerde<DailyExpenditureResponse> {

    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<DailyExpenditureResponse> {
    }
}
