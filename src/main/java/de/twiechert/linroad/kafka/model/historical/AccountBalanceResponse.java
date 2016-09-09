package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Sextet;


/**
 * This class represents the response object for the account balance request.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccountBalanceResponse extends Sextet<Integer, Long, Long, Long, Integer, Double> {

    /**
     * Default constructor may be required depending or serialization library
     */
    public AccountBalanceResponse() {
    }

    public AccountBalanceResponse(Long requestTime, Long responseTime, Long resultTime, Integer queryId, Double balance) {
        super(2, requestTime, responseTime, resultTime, queryId, balance);
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
    public long getResultTIme() {
        return getValue3();
    }

    @JsonIgnore
    public int getQueryId() {
        return getValue4();
    }

    @JsonIgnore
    public double getBalance() {
        return getValue5();
    }



}
