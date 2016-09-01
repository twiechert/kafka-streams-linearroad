package de.twiechert.linroad.kafka.stream.windowing;

import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tafyun on 02.09.16.
 */
public class LavWindow extends Windows<TimeWindow> {


    /**
     * The size of the window, i.e. how long a window lasts.
     * The window size's effective time unit is determined by the semantics of the topology's
     * configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long size = 300;

    /**
     * The size of the window's advance interval, i.e. by how much a window moves forward relative
     * to the previous one. The interval's effective time unit is determined by the semantics of
     * the topology's configured {@link org.apache.kafka.streams.processor.TimestampExtractor}.
     */
    public final long advance = 60;

    private LavWindow(String name) {
        super(name);
    }

    /**
     * Returns a window definition with the given window size, and with the advance interval being
     * equal to the window size. Think: [N * size, N * size + size), with N denoting the N-th
     * window.
     * <p>
     * This provides the semantics of tumbling windows, which are fixed-sized, gap-less,
     * non-overlapping windows. Tumbling windows are a specialization of hopping windows.
     *
     * @param name The name of the window. Must not be null or empty.
     *             }.
     * @return a new window definition
     */
    public static LavWindow of(String name) {
        return new LavWindow(name);
    }


    @Override
    public Map<Long, TimeWindow> windowsFor(long timestamp) {
        long windowStart = (Math.max(0, timestamp - this.size + this.advance) / this.advance) * this.advance;
        Map<Long, TimeWindow> windows = new HashMap<>();
        while (windowStart <= timestamp) {
            TimeWindow window = new TimeWindow(windowStart, windowStart + this.size);
            windows.put(windowStart, window);
            windowStart += this.advance;
        }

        if (timestamp < 300) {
            for (long i = (300 - 60); i >= timestamp; i -= 60) {
                TimeWindow window = new TimeWindow(0l, i);
                windows.put(0l, window);


            }
        }
        return windows;
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TimeWindows)) {
            return false;
        }
        TimeWindows other = (TimeWindows) o;
        return this.size == other.size && this.advance == other.advance;
    }

    @Override
    public int hashCode() {
        int result = (int) (size ^ (size >>> 32));
        result = 31 * result + (int) (advance ^ (advance >>> 32));
        return result;
    }
}
