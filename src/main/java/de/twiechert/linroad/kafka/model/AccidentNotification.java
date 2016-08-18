package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Quartet;

/**
 * Created by tafyun on 02.08.16.
 */
public class AccidentNotification extends Quartet<Integer, Long, Long, Integer> {

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


    public static class Serde extends DefaultSerde<AccidentNotification> {

    }

}
