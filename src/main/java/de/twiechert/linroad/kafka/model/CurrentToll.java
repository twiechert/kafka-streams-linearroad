package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Triplet;

/**
 * Represents a toll valid in a segment bound to a certain minute and based on the average velocity in that segment.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class CurrentToll extends Triplet<Long, Double, Double> implements TimedOnMinute {

    /**
     * Default constructor may be required depending or serialization library
     */
    public CurrentToll() {
    }

    public CurrentToll(Long minute, Double toll, Double velocity) {
        super(minute, toll, velocity);
    }


    @JsonIgnore
    public double getToll() {
        return getValue1();
    }

    @JsonIgnore
    public double getVelocity() {
        return getValue2();
    }

    @Override
    @JsonIgnore
    public long getMinute() {
        return getValue0();
    }

}
