package de.twiechert.linroad.kafka.stream.processor;

import de.twiechert.linroad.kafka.model.TimedOnMinute;

import java.io.Serializable;

/**
 * Created by tafyun on 03.09.16.
 */
public class TimedKey<Key> implements TimedOnMinute, Comparable<TimedKey<Key>>, Serializable {

    private Key key;

    private Long minute;


    public TimedKey() {

    }

    public TimedKey(Key key, long minute) {
        this.key = key;
        this.minute = minute;
    }

    @Override
    public long getMinute() {
        return minute;
    }

    public Key getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimedKey<?> timedKey = (TimedKey<?>) o;

        if (minute != timedKey.minute) return false;
        return key.equals(timedKey.key);

    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (int) (minute ^ (minute >>> 32));
        return result;
    }

    @Override
    public int compareTo(TimedKey<Key> other) {
        if (this.equals(other))
            return 0;
        else
            return minute.compareTo(other.getMinute());
    }
}