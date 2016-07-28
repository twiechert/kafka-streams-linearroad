package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Pair;
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


        public Long getTime(){
            return getValue0();
        }

        public Integer getVehicleId(){
            return getValue1();
        }
    }



    public static class Value extends Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>
    {
        public Value(Integer value0, Integer value1, Integer value2, Boolean value3, Integer value4, Integer value5) {
            super(value0, value1, value2, value3, value4, value5);
        }

        public Integer getSpeed(){
            return getValue0();
        }

        public Integer getXway(){
            return getValue1();
        }

        public Integer getLane(){
            return getValue2();
        }

        public Boolean getDir(){
            return getValue3();
        }

        public Integer getSeg(){
            return getValue4();
        }

        public Integer getPos(){
            return getValue5();
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
