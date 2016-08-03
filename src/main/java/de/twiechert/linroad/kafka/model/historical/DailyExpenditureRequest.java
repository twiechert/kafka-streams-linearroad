package de.twiechert.linroad.kafka.model.historical;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Quintet;

/**
 * Created by tafyun on 02.08.16.
 */
public class DailyExpenditureRequest extends Quintet<Long, Integer, Integer, Integer, Integer> {

    public DailyExpenditureRequest(Long requestTime, Integer vehicleId, Integer queryId, Integer xway, Integer day) {
        super(requestTime, vehicleId, queryId, xway, day);
    }


    public long getRequestTime() {
        return getValue0();
    }

    public int getVehicleId() {
        return getValue1();
    }

    public int getQueryId() {
        return getValue2();
    }

    public int getXWay() {
        return getValue3();
    }

    public int getDay() {
        return getValue3();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }


    public static class Serde extends ByteArraySerde<DailyExpenditureRequest> {
    }

    public static class Serializer
            extends ByteArraySerde.BArraySerializer<DailyExpenditureRequest> {
    }
}
