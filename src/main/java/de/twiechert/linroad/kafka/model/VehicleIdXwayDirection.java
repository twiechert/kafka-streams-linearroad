package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Quartet;
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


    public Integer getVehicleId() {
        return getValue0();
    }

    public Integer getXway() {
        return getValue1();
    }

    public Boolean getDir() {
        return getValue2();
    }


    public static class Serde extends ByteArraySerde<VehicleIdXwayDirection> {
    }


}
