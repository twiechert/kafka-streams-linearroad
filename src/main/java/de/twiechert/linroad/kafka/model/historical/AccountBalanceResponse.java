package de.twiechert.linroad.kafka.model.historical;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Quintet;
import org.javatuples.Sextet;
import org.javatuples.Triplet;


/**
 * This class represents the response object for the account balance request.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccountBalanceResponse extends Sextet<Integer, Long, Long, Long, Integer, Double> {

    public AccountBalanceResponse(Long requestTime, Long responseTime, Long resultTime, Integer queryId, Double balance) {
        super(2, requestTime, responseTime, resultTime, queryId, balance);
    }

    public long getRequestTime() {
        return getValue1();
    }

    public long getResponseTime() {
        return getValue2();
    }

    public long getResultTIme() {
        return getValue3();
    }

    public int getQueryId() {
        return getValue4();
    }


    public double getBalance() {
        return getValue5();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends ByteArraySerde<AccountBalanceResponse> {
    }

    public static class Serializer
            extends ByteArraySerde.BArraySerializer<AccountBalanceResponse> {
    }
}
