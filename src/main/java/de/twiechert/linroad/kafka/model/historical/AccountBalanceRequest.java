package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 02.08.16.
 */
public class AccountBalanceRequest extends Triplet<Long, Integer, Integer> {


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

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends DefaultSerde<AccountBalanceRequest> {
        public Serde() {
            super(AccountBalanceRequest.class);
        }
    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<AccountBalanceRequest> {
    }
}
