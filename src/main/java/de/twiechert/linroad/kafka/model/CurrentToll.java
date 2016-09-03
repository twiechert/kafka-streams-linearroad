package de.twiechert.linroad.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import org.javatuples.Triplet;

/**
 * Represents a toll valid in a segment bound to a certain minute and based on the average velocity in that segment.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class CurrentToll extends Triplet<Long, Double, Double> {

    public CurrentToll() {
    }

    public CurrentToll(Long time, Double toll, Double velocity) {
        super(time, toll, velocity);
    }

    @JsonIgnore
    public long getTime() {
        return getValue0();
    }

    @JsonIgnore
    public double getToll() {
        return getValue1();
    }

    @JsonIgnore
    public double getVelocity() {
        return getValue2();
    }

    public static class Serde extends DefaultSerde<CurrentToll> {}

}
