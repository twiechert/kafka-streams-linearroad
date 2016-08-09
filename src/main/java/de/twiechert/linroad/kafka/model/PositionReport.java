package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Quintet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class PositionReport extends Quintet<Long, Integer, Integer, Integer, Integer> implements Serializable {


    public PositionReport() {

    }

    public PositionReport(Long time, Integer vehicleId, Integer speed, Integer lane, Integer pos) {
        super(time, vehicleId, speed, lane, pos);
    }

    @JsonIgnore
    public Long getTime() {
        return getValue0();
    }

    @JsonIgnore
    public Integer getVehicleId() {
        return getValue1();
    }


    @JsonIgnore
    public Integer getSpeed() {
        return getValue2();
    }


    @JsonIgnore
    public Integer getLane() {
        return getValue3();
    }


    @JsonIgnore
    public Integer getPos() {
        return getValue4();
    }


    public static class Serializer
            extends DefaultSerde.DefaultSerializer<PositionReport> {


    }


}
