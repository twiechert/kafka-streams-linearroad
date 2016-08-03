package de.twiechert.linroad.kafka.model;

import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.javatuples.Quartet;

/**
 * Created by tafyun on 02.08.16.
 */
public class AccidentNotification extends Quartet<Integer, Long, Long, Integer> {
    public AccidentNotification(Long requestTime, Long responseTime, Integer segment) {
        super(1, requestTime, responseTime, segment);
    }

    public long getRequestTime() {
        return getValue1();
    }


    public long getResponseTIme() {
        return getValue2();
    }


    public int getSegment() {
        return getValue3();
    }


    public static class Serde extends ByteArraySerde<AccidentNotification> {
    }

    @Override
    public String toString() {
        return "AccidentNotification->" + super.toString();
    }
}
