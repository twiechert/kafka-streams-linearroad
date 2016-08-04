package de.twiechert.linroad.kafka.model.historical;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 02.08.16.
 */
public class XwayVehicleDay extends Triplet<Integer, Integer, Integer> {

    public XwayVehicleDay() {
    }

    public XwayVehicleDay(Integer xway, Integer vehicleId, Integer day) {
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

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends DefaultSerde<XwayVehicleDay> {
        public Serde() {
            super(XwayVehicleDay.class);
        }
    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<XwayVehicleDay> {
    }
}
