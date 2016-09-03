package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Pair;

import java.io.Serializable;

/**
 * Represents an average velocity bound to a certain timestamp.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AverageVelocity extends Pair<Long, Double> implements Serializable, TimedOnMinute {

    public AverageVelocity() {
    }

    public AverageVelocity(Long minute, Double velocity) {
        super(minute, velocity);
    }

    @JsonIgnore
    public long getMinute() {
        return getValue0();
    }

    @JsonIgnore
    public double getAverageSpeed() {
        return getValue1();
    }


}
