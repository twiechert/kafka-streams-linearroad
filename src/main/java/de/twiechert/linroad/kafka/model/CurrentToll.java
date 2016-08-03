package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 01.08.16.
 */
public class CurrentToll extends Triplet<Long, Double, Double> {


    public CurrentToll(Long time, Double toll, Double velocity) {
        super(time, toll, velocity);
    }

    public long getTime() {
        return getValue0();
    }

    public double getToll() {
        return getValue1();
    }

    public double getVelocity() {
        return getValue2();
    }

    public static class Serde extends ByteArraySerde<CurrentToll> {
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }


}
