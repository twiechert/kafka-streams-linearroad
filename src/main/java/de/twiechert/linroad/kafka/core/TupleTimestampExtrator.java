package de.twiechert.linroad.kafka.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Tuple;

/**
 * This class is able to extract timestamps from tuples.
 * For that purpose you specify, if the timestamp is read from the key or value and the position within the tuple (beginning with 0)
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class TupleTimestampExtrator extends FallbackTimestampExtractor implements TimestampExtractor {


    private final KeyValue keyValue;

    private final int pos;

    /**
     *
     * @param keyValue the part of the message to extract the timestamp from
     * @param pos the position to extract the timestamp from
     */
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
