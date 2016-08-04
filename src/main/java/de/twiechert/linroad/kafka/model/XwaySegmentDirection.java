package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 * Created by tafyun on 29.07.16.
 */
public class XwaySegmentDirection extends Triplet<Integer, Integer, Boolean> implements Serializable{

    public XwaySegmentDirection(Integer xway, Integer seg, Boolean dir) {
        super(xway, seg, dir);
    }

    public XwaySegmentDirection() {
    }

    @JsonIgnore
    public Integer getXway(){
        return getValue0();
    }


    @JsonIgnore
    public Integer getSeg(){
        return getValue1();
    }

    @JsonIgnore
    public Boolean getDir() {
        return getValue2();
    }


    public static class Serde extends DefaultSerde<XwaySegmentDirection> {
        public Serde() {
            super(XwaySegmentDirection.class);
        }
    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<XwaySegmentDirection> {
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " -> " + super.toString();
    }

}
