package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class VehicleIdXwayDirection extends Triplet<Integer, Integer, Boolean> implements Serializable {


    public VehicleIdXwayDirection(Integer vehicleId, Integer xway, Boolean dir) {
        super(vehicleId, xway, dir);
    }

    public VehicleIdXwayDirection(Integer vehicleId, XwaySegmentDirection xwaySegmentDirection) {
        super(vehicleId, xwaySegmentDirection.getXway(), xwaySegmentDirection.getDir());
    }

    public VehicleIdXwayDirection() {

    }


    @JsonIgnore
    public Integer getVehicleId() {
        return getValue0();
    }

    @JsonIgnore
    public Integer getXway() {
        return getValue1();
    }

    @JsonIgnore
    public Boolean getDir() {
        return getValue2();
    }




}
