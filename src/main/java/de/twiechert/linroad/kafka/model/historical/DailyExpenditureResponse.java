package de.twiechert.linroad.kafka.model.historical;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Quintet;
import org.javatuples.Sextet;

import java.io.Serializable;

/**
 * Created by tafyun on 02.08.16.
 */
public class DailyExpenditureResponse extends Quintet<Integer, Long, Long, Integer, Double> {

    public DailyExpenditureResponse(Long requestTime, Long responseTime, Integer queryId, Double balance) {
        super(3, requestTime, responseTime, queryId, balance);
    }

    public long getRequestTime() {
        return getValue1();
    }

    public long getResponseTime() {
        return getValue2();
    }

    public int getQueryId() {
        return getValue3();
    }

    public double getBalance() {
        return getValue4();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends ByteArraySerde<DailyExpenditureResponse> {
    }

    public static class Serializer
            extends ByteArraySerde.BArraySerializer<DailyExpenditureResponse> {
    }
}
