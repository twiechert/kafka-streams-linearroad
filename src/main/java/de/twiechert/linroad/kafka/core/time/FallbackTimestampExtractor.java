package de.twiechert.linroad.kafka.core.time;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by tafyun on 21.07.16.
 */
public abstract class FallbackTimestampExtractor implements TimestampExtractor {


    private WallclockTimestampExtractor wallclockTimestampExtractor = new WallclockTimestampExtractor();

    private static final Logger logger = LoggerFactory
            .getLogger(FallbackTimestampExtractor.class);


    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        try {
            return this.extractTimestamp(record);
        } catch (Exception e) {
            /*
            logger.debug("Using fallback timestamp. Classes are {}, {}",
                    record.key().getClass().getCanonicalName(),
                    record.value().getClass().getCanonicalName());
*/
            return wallclockTimestampExtractor.extract(record);
        }
    }

    public abstract long extractTimestamp(ConsumerRecord<Object, Object> record);

}