package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

import java.io.Serializable;

/**
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class XwaySegmentDirection extends Triplet<Integer, Integer, Boolean> implements Serializable{

    public XwaySegmentDirection(Integer xway, Integer seg, Boolean dir) {
        super(xway, seg, dir);
    }

    /**
     * Default constructor may be required depending or serialization library
     */
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
    }

    public static class Serializer
            extends DefaultSerde.DefaultSerializer<XwaySegmentDirection> {
    }


}
