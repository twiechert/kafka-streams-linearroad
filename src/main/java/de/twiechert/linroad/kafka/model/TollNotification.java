package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
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


    public int getVehicleId() {
        return getValue1();
    }

    public long getRequestTime() {
        return getValue2();
    }

    public long getResponseTime() {
        return getValue3();
    }

    public double getVelocity() {
        return getValue4();
    }

    public double getToll() {
        return getValue5();
    }

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

    public static class Serde extends ByteArraySerde<TollNotification> {
    }

}
