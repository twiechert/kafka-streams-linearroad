package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleDay;
import org.javatuples.Septet;
import org.javatuples.Sextet;

/**
 * Created by tafyun on 01.08.16.
 */
public class TollNotification extends Septet<Integer, Integer, Long, Long, Double, Double, Integer> {

    public TollNotification(Integer vehicleId, Long reqTime, Long respTime, Double velocity, Double toll, Integer xway) {
        super(0, vehicleId, reqTime, respTime, velocity, toll, xway);
        // the xway is not required in the output tuple, but we use this stream for further table processing
    }

    public TollNotification(Integer vehicleId, Long reqTime, Long respTime, Double velocity, Double toll) {
        super(0, vehicleId, reqTime, respTime, velocity, toll, 0);
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
    public long getResponseTime() {
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

    @JsonIgnore
    public int getXway() {
        return getValue6();
    }

    public TollNotification setXway(int xway) {
        this.setAt6(xway);
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends DefaultSerde<TollNotification> {
        public Serde() {
            super(TollNotification.class);
        }
    }

}
