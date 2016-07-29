package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class XwaySegmentDirection extends Triplet<Integer, Integer, Boolean> implements Serializable{
    public XwaySegmentDirection(Integer xway, Integer seg, Boolean dir) {
        super(xway, seg, dir);
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

    public static class Serializer
            extends ByteArraySerde.BArraySerializer<XwaySegmentDirection> {
    }



}
