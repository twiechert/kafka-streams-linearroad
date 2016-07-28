package de.twiechert.linroad.kafka.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Tuple;

/**
 * This class is able to extract timestamps from tuples
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TupleTimestampExtrator extends FallbackTimestampExtractor implements TimestampExtractor {


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
    public long extractTimestamp(ConsumerRecord<Object, Object> record) {
        if(keyValue.equals(KeyValue.Key)) {
            if(record.key() instanceof Tuple)
                return (long) ((Tuple) record.key()).getValue(pos);
            else return (long)  record.key();

        } else {
            if(record.value() instanceof Tuple)
                return (long) ((Tuple) record.value()).getValue(pos);
            else return (long)  record.value();
        }
    }
}
