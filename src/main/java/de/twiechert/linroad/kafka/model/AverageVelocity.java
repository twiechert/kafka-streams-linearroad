package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 * Represents an average velocity bound to a certain timestamp.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AverageVelocity extends Triplet<Long, Double, Long> implements Serializable, TimedOnMinute.TimedOnMinuteWithWindowEnd {

    /**
     * Default constructor may be required depending or serialization library
     */
    public AverageVelocity() {
    }

    public AverageVelocity(Long windowEndMinute, Double velocity, Long posReportTime) {
        super(windowEndMinute, velocity, posReportTime);
    }

    @JsonIgnore
    public long getMinute() {
        return getValue2();
    }

    @JsonIgnore
    public double getAverageSpeed() {
        return getValue1();
    }

    @Override
    public long getWindowEndMinute() {
        return getValue0();
    }
}
