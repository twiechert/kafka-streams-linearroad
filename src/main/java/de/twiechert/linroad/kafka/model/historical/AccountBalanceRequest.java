package de.twiechert.linroad.kafka.model.historical;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 02.08.16.
 */
public class AccountBalanceRequest extends Triplet<Long, Integer, Integer> {

    public AccountBalanceRequest(Long requestTime, Integer vehicleId, Integer queryId) {
        super(requestTime, vehicleId, queryId);
    }

    public Long getRequestTime() {
        return getValue0();
    }

    public Integer getVehicleID() {
        return getValue1();
    }

    public Integer getQueryId() {
        return getValue2();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends ByteArraySerde<AccountBalanceRequest> {
    }

    public static class Serializer
            extends ByteArraySerde.BArraySerializer<AccountBalanceRequest> {
    }
}
