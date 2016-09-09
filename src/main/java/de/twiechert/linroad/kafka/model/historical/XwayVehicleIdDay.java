package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

/**
 * This class represents an (Xway,VehicleId,Day) tuple.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class XwayVehicleIdDay extends Triplet<Integer, Integer, Integer> {

    /**
     * Default constructor may be required depending or serialization library
     */
    public XwayVehicleIdDay() {
    }

    public XwayVehicleIdDay(Integer xway, Integer vehicleId, Integer day) {
        super(xway, vehicleId, day);
    }

    @JsonIgnore
    public int getXway() {
        return getValue0();
    }

    @JsonIgnore
    public int getVehicleId() {
        return getValue1();
    }

    @JsonIgnore
    public int getDay() {
        return getValue2();
    }


    public static class Serializer
            extends DefaultSerde.DefaultSerializer<XwayVehicleIdDay> {
    }
}
