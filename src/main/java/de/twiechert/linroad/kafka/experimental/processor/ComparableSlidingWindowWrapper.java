package de.twiechert.linroad.kafka.experimental.processor;

import org.apache.kafka.streams.kstream.Windowed;

import java.io.Serializable;

/**
 * Created by tafyun on 30.08.16.
 */
public class ComparableSlidingWindowWrapper<V> implements Comparable<ComparableSlidingWindowWrapper>, Serializable {

    private V key;


    private long end;


    public ComparableSlidingWindowWrapper(Windowed<V> key) {
        this.key = key.key();
        this.end = key.window().end();
    }

    public ComparableSlidingWindowWrapper(long end) {
        this.end = end;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof ComparableSlidingWindowWrapper))
            return false;

        ComparableSlidingWindowWrapper that = (ComparableSlidingWindowWrapper) obj;

        return this.end == that.end && this.key.equals(that.key);
    }

    /*
    @Override
    public int hashCode() {
        long n = (end << 32) | key.hashCode();
        return (int) (n % 0xFFFFFFFFL);
    }*/

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }

    @Override
    public int compareTo(ComparableSlidingWindowWrapper o) {
        if (this.end < o.end) {
            return -1;
        } else if (this.end > o.end) {
            return 1;
        }

        return (o.key != null && this.key != null && this.key.equals(o.key)) ? 0 : -1;
    }

    public V getKey() {
        return key;
    }

    public long getEnd() {
        return end;
    }


}
