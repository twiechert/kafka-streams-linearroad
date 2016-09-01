package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class AverageVelocity extends Pair<Long, Double> implements Serializable {


    public AverageVelocity() {

    }

    public AverageVelocity(Long value0, Double value1) {
        super(value0, value1);
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
