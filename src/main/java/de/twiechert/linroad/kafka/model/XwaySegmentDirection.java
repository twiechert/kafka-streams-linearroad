package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class XwaySegmentDirection extends Triplet<Integer, Integer, Boolean> implements Serializable{
    public XwaySegmentDirection(Integer value0, Integer value1, Boolean value2) {
        super(value0, value1, value2);
    }



    public Integer getXway(){
        return getValue0();
    }

    public Boolean getDir(){
        return getValue2();
    }

    public Integer getSeg(){
        return getValue1();
    }

    public static class Serde extends ByteArraySerde<XwaySegmentDirection> {
    }


}
