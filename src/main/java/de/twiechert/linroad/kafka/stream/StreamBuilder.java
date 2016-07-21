package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.feeder.DataFeeder;
import de.twiechert.linroad.kafka.core.feeder.PositionReportHandler;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;

import java.util.Properties;

/**
 * Created by tafyun on 02.06.16.
 */
public abstract  class StreamBuilder<OutputKey, OutputValue> {



    private Properties streamConfig = new Properties();

    private final Serde<OutputKey> keySerde;

    private final Serde<OutputValue> valueSerde;

    protected Util util;

    protected LinearRoadKafkaBenchmarkApplication.Context context;

    @Autowired
    public StreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util,  Serde<OutputKey> keySerde,  Serde<OutputValue> valueSerde) {
        this.util = util;
        this.context = context;
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-"+this.context.getApplicationId());
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092");
        streamConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.2:2181");
        streamConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, PositionReportHandler.PositionReportKeySerde.class.getName());
        streamConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, PositionReportHandler.PositionReportValueSerde.class.getName());
        streamConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReportHandler.TimeStampExtractor.class.getName());

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }


    @Async
    public void startStream() {
        this.startStream(false);
    }

    @Async
    public void startStream(boolean log) {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<OutputKey, OutputValue> stream = this.getStream(builder);
        if(log) {
            stream.print();
        }
        KafkaStreams kafkaStreams = new KafkaStreams(builder, this.getStreamConfig());
        stream.to(this.keySerde, this.valueSerde, getOutputTopic());
        if(this.getOptions().getFileoutput()!=null) {
            stream.writeAsText(this.getOptions().getFileoutput());
        }
        kafkaStreams.start();
    }

     protected abstract KStream<OutputKey, OutputValue> getStream(KStreamBuilder builder);



    protected Properties getStreamConfig() {
        return this.getBaseProperties();
    }

    protected Properties getBaseProperties() {
        return streamConfig;
    }

    protected Serde<OutputKey> getKeySerde() {
        return keySerde;
    }

    protected Serde<OutputValue> getValueSerde() {
        return valueSerde;
    }

    abstract protected String getOutputTopic();


    protected Options getOptions() {
        return new Options(null);
    }

    protected static class Options {
        private String fileoutput = null;

        public Options(String fileoutput) {
            this.fileoutput = fileoutput;
        }

        public String getFileoutput() {
            return fileoutput;
        }

        public void setFileoutput(String fileoutput) {
            this.fileoutput = fileoutput;
        }

    }
}
