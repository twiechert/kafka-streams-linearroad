package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Pair;

import java.io.Serializable;

/**
 * Represents the number of distinct vehicles (in a certain segment) bound to a certain timestamp.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class NumberOfVehicles extends Pair<Long, Integer> implements Serializable, TimedOnMinute {

    public NumberOfVehicles() {
    }

    public NumberOfVehicles(Long time, Integer numberOfVehiles) {
        super(time, numberOfVehiles);
    }

    @JsonIgnore
    @Override
    public long getMinute() {
        return getValue0();
    }

    @JsonIgnore
    public Integer getNumberOfVehicles() {
        return getValue1();
    }


}
