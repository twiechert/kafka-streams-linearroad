package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Pair;

import java.io.Serializable;

/**
 * Represents an average velocity bound to a certain timestamp.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AverageVelocity extends Pair<Long, Double> implements Serializable {

    public AverageVelocity() {
    }

    public AverageVelocity(Long time, Double velocity) {
        super(time, velocity);
    }

    @JsonIgnore
    public Long getMinute() {
        return getValue0();
    }

    @JsonIgnore
    public Double getAverageSpeed() {
        return getValue1();
    }


}
