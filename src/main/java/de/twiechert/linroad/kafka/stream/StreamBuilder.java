package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;

import java.util.Properties;

/**
 * Created by tafyun on 02.06.16.
 */
public abstract class StreamBuilder<OutputKey, OutputValue> {


    private Properties streamConfig = new Properties();

    private final Serde<OutputKey> keySerde;

    private final Serde<OutputValue> valueSerde;

    protected Util util;

    protected LinearRoadKafkaBenchmarkApplication.Context context;

    public StreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util, Serde<OutputKey> keySerde, Serde<OutputValue> valueSerde) {
        this.util = util;
        this.context = context;
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-"+this.context.getApplicationId());
        streamConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReportHandler.TimeStampExtractor.class.getName());

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }




    public Serde<OutputKey> getKeySerde() {
        return keySerde;
    }

    public Serde<OutputValue> getValueSerde() {
        return valueSerde;
    }

    public abstract String getOutputTopic();


}
