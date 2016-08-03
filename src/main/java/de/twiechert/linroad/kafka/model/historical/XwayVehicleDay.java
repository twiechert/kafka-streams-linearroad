package de.twiechert.linroad.kafka.model.historical;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 02.08.16.
 */
public class XwayVehicleDay extends Triplet<Integer, Integer, Integer> {

    public XwayVehicleDay(Integer xway, Integer vehicleId, Integer day) {
        super(xway, vehicleId, day);
    }

    public int getXway() {
        return getValue0();
    }

    public int getVehicleId() {
        return getValue1();
    }

    public int getDay() {
        return getValue2();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

    public static class Serde extends ByteArraySerde<XwayVehicleDay> {
    }

}
