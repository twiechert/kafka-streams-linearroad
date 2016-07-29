package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Pair;
import org.javatuples.Quintet;
import org.javatuples.Sextet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class PositionReport implements Serializable {


    public static class Key extends Pair<Long, Integer> {
        public Key(Long value0, Integer value1) {
            super(value0, value1);
        }



    }



    public static class Value extends Quintet<Long, Integer, Integer, Integer, Integer>
    {

        public Value(Long time, Integer vehicleId, Integer speed, Integer lane, Integer pos) {
            super(time, vehicleId, speed, lane, pos);
        }


        public Long getTime(){
            return getValue0();
        }


        public Integer getVehicleId(){
            return getValue1();
        }



        public Integer getSpeed(){
            return getValue2();
        }



        public Integer getLane(){
            return getValue3();
        }



        public Integer getPos(){
            return getValue4();
        }
    }


    public static class KeySerializer
            extends ByteArraySerde.BArraySerializer<PositionReport.Key> {
    }

    public static class ValueSerializer
            extends ByteArraySerde.BArraySerializer<PositionReport.Value> {
    }

    public static class KeySerde extends ByteArraySerde<PositionReport.Key> {
    }

    public static class ValueSerde extends ByteArraySerde<PositionReport.Value> {
    }
}
