package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Quintet;

/**
 * This class represents the response object for the daily expenditure query (type 3).
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class DailyExpenditureResponse extends Quintet<Integer, Long, Long, Integer, Double> {

    /**
     * Default constructor may be required depending or serialization library
     */
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

}
