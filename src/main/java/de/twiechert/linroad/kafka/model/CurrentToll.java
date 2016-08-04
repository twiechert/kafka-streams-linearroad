package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 01.08.16.
 */
public class CurrentToll extends Triplet<Long, Double, Double> {


    public CurrentToll() {

    }

    public CurrentToll(Long time, Double toll, Double velocity) {
        super(time, toll, velocity);
    }

    @JsonIgnore
    public long getTime() {
        return getValue0();
    }

    @JsonIgnore
    public double getToll() {
        return getValue1();
    }

    @JsonIgnore
    public double getVelocity() {
        return getValue2();
    }

    public static class Serde extends DefaultSerde<CurrentToll> {

        public Serde() {
            super(CurrentToll.class);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }


}
