package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Quintet;

/**
 * Created by tafyun on 02.08.16.
 */
public class DailyExpenditureRequest extends Quintet<Long, Integer, Integer, Integer, Integer> {

    public DailyExpenditureRequest() {
    }

    public DailyExpenditureRequest(Long requestTime, Integer vehicleId, Integer queryId, Integer xway, Integer day) {
        super(requestTime, vehicleId, queryId, xway, day);
    }


    @JsonIgnore
    public long getRequestTime() {
        return getValue0();
    }

    @JsonIgnore
    public int getVehicleId() {
        return getValue1();
    }

    @JsonIgnore
    public int getQueryId() {
        return getValue2();
    }

    @JsonIgnore
    public int getXWay() {
        return getValue3();
    }

    @JsonIgnore
    public int getDay() {
        return getValue3();
    }


    public static class Serde extends DefaultSerde<DailyExpenditureRequest> {

    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<DailyExpenditureRequest> {
    }
}
