package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Sextet;

/**
 * Created by tafyun on 01.08.16.
 */
public class TollNotification extends Sextet<Integer, Integer, Long, Long, Double, Double> {

    public TollNotification(Integer vehicleId, Long reqTime, Long emitTime, Double velocity, Double toll) {
        super(0, vehicleId, reqTime, emitTime, velocity, toll);
        // the xway is not required in the output tuple, but we use this stream for further table processing
    }

    public TollNotification() {

    }

    @JsonIgnore
    public int getVehicleId() {
        return getValue1();
    }

    @JsonIgnore
    public long getRequestTime() {
        return getValue2();
    }

    @JsonIgnore
    public long getEmitTime() {
        return getValue3();
    }

    @JsonIgnore
    public double getVelocity() {
        return getValue4();
    }

    @JsonIgnore
    public double getToll() {
        return getValue5();
    }


}
