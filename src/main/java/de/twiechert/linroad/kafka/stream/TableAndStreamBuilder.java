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
public abstract class TableAndStreamBuilder<OutputKey, OutputValue> {

    public abstract static class StreamBuilder<OutputKey1, OutputValue1> extends TableAndStreamBuilder<OutputKey1, OutputValue1> {
        public StreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util, Serde<OutputKey1> keySerde, Serde<OutputValue1> valueSerde) {
            super(context, util, keySerde, valueSerde);
        }

        @Override
        protected KTable<OutputKey1, OutputValue1> getTable(KStreamBuilder builder) {
            return null;
        }
    }

    public abstract static class TableBuilder<OutputKey1, OutputValue1> extends TableAndStreamBuilder<OutputKey1, OutputValue1> {
        public TableBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util, Serde<OutputKey1> keySerde, Serde<OutputValue1> valueSerde) {
            super(context, util, keySerde, valueSerde);
        }

        @Override
        protected KStream<OutputKey1, OutputValue1> getStream(KStreamBuilder builder) {
            return null;
        }
    }

    private Properties streamConfig = new Properties();

    private final Serde<OutputKey> keySerde;

    private final Serde<OutputValue> valueSerde;

    protected Util util;

    protected LinearRoadKafkaBenchmarkApplication.Context context;

    public TableAndStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util, Serde<OutputKey> keySerde, Serde<OutputValue> valueSerde) {
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
        KTable<OutputKey, OutputValue> table = this.getTable(builder);

        if(stream!=null) {
            if(log) {
                stream.print();
            }
            stream.to(this.keySerde, this.valueSerde, getOutputTopic());

            if(this.getOptions().getFileoutput()!=null) {
                stream.writeAsText(this.getOptions().getFileoutput());
            }
        }

        if(table!=null) {
            if(log) {
                table.print();
            }
            table.to(this.keySerde, this.valueSerde, getOutputTopic());

            if(this.getOptions().getFileoutput()!=null) {
                table.writeAsText(this.getOptions().getFileoutput());
            }
        }
        KafkaStreams kafkaStreams = new KafkaStreams(builder, this.getStreamConfig());
        kafkaStreams.start();


    }

     protected abstract KStream<OutputKey, OutputValue> getStream(KStreamBuilder builder);

    protected abstract KTable<OutputKey, OutputValue> getTable(KStreamBuilder builder);


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
