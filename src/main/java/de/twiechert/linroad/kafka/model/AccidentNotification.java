package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.historical.XwayVehicleDay;
import org.javatuples.Quartet;

/**
 * Created by tafyun on 02.08.16.
 */
public class AccidentNotification extends Quartet<Integer, Long, Long, Integer> {

    public AccidentNotification() {
    }

    public AccidentNotification(Long requestTime, Long responseTime, Integer segment) {
        super(1, requestTime, responseTime, segment);
    }

    @JsonIgnore
    public long getRequestTime() {
        return getValue1();
    }

    @JsonIgnore
    public long getResponseTIme() {
        return getValue2();
    }

    @JsonIgnore
    public int getSegment() {
        return getValue3();
    }


    public static class Serde extends DefaultSerde<AccidentNotification> {
        public Serde() {
            super(AccidentNotification.class);
        }
    }

    @Override
    public String toString() {
        return "AccidentNotification->" + super.toString();
    }
}
