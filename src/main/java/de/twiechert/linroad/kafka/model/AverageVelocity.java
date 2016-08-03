package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Pair;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class AverageVelocity extends Pair<Long, Double> implements Serializable {


    public AverageVelocity(Long value0, Double value1) {
        super(value0, value1);
    }

    public Long getMinute() {
        return getValue0();
    }

    public Double getAverageSpeed() {
        return getValue1();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }
    public static class Serde extends ByteArraySerde<AverageVelocity> {
    }
}
