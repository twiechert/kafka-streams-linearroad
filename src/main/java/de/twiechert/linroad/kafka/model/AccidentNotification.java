package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Quartet;


/**
 * This class represents an accident notification according to the LR requirements.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccidentNotification extends Quartet<Integer, Long, Long, Integer> {

    /**
     * Default constructor may be required depending or serialization library
     */
    public AccidentNotification() {
    }

    public AccidentNotification(Long occurenceTime, Long emitTime, Integer segment) {
        super(1, occurenceTime, emitTime, segment);
    }

    @JsonIgnore
    public long getOccurenceTime() {
        return getValue1();
    }

    @JsonIgnore
    public long getEmitTime() {
        return getValue2();
    }

    @JsonIgnore
    public int getSegment() {
        return getValue3();
    }

}
