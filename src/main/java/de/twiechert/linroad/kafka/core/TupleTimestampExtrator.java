package de.twiechert.linroad.kafka.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Tuple;

/**
 * This class is able to extract timestamps from tuples
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TupleTimestampExtrator implements TimestampExtractor{


    private final KeyValue keyValue;

    private final int pos;

    public TupleTimestampExtrator(KeyValue keyValue, int pos) {
        this.keyValue = keyValue;
        this.pos = pos;
    }

    public enum KeyValue {
        Key, Value
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
            return (keyValue.equals(KeyValue.Key)) ?  (long) (( Tuple) record.key()).getValue(pos) : (long) (( Tuple) record.value()).getValue(pos);
    }
}
