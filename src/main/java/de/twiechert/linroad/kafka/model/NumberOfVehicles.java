package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Pair;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class NumberOfVehicles extends Pair<Long, Integer> implements Serializable {


    public NumberOfVehicles() {

    }

    public NumberOfVehicles(Long value0, Integer value1) {
        super(value0, value1);
    }

    @JsonIgnore
    public Long getMinute() {
        return getValue0();
    }

    @JsonIgnore
    public Integer getNumber() {
        return getValue1();
    }


}
