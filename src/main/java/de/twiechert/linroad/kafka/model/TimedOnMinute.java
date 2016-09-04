package de.twiechert.linroad.kafka.model;

/**
 * Created by tafyun on 03.09.16.
 */
public interface TimedOnMinute {

    long getMinute();

    interface TimedOnMinuteWithWindowEnd extends TimedOnMinute {
        long getWindowEndMinute();
    }
}
