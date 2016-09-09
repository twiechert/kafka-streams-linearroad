package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

/**
 * This class represents the account balance request object (query type 2).
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccountBalanceRequest extends Triplet<Long, Integer, Integer> {


    /**
     * Default constructor may be required depending or serialization library
     */
    public AccountBalanceRequest() {
    }

    public AccountBalanceRequest(Long requestTime, Integer vehicleId, Integer queryId) {
        super(requestTime, vehicleId, queryId);
    }

    @JsonIgnore
    public Long getRequestTime() {
        return getValue0();
    }

    @JsonIgnore
    public Integer getVehicleID() {
        return getValue1();
    }

    @JsonIgnore
    public Integer getQueryId() {
        return getValue2();
    }


    public static class Serde extends DefaultSerde<AccountBalanceRequest> {
    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<AccountBalanceRequest> {

    }
}
