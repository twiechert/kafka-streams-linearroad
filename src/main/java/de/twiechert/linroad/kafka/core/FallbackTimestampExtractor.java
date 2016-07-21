package de.twiechert.linroad.kafka.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

/**
 * Created by tafyun on 21.07.16.
 */
public abstract class FallbackTimestampExtractor implements TimestampExtractor {


    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        try {
            return this.extractTimestamp(record);
        } catch (Exception e) {
            return new WallclockTimestampExtractor().extract(record);
        }
    }

    public abstract long extractTimestamp(ConsumerRecord<Object, Object> record);

}