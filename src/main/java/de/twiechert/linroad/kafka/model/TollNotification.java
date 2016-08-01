package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Sextet;

/**
 * Created by tafyun on 01.08.16.
 */
public class TollNotification extends Sextet<Integer, Integer, Long, Long, Double, Double> {

    public TollNotification(Integer vehicleId, Long reqTime, Long respTime, Double velocity, Double toll) {
        super(0, vehicleId, reqTime, respTime, velocity, toll);
    }


    public static class Serde extends ByteArraySerde<TollNotification> {
    }

}
