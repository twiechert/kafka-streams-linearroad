package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleDay;
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

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends DefaultSerde<VehicleIdXwayDirection> {
        public Serde() {
            super(VehicleIdXwayDirection.class);
        }
    }


}
