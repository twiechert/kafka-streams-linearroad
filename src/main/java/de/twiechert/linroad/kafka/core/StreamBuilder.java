package de.twiechert.linroad.kafka.core;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.PositionReporter;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Properties;

/**
 * Created by tafyun on 02.06.16.
 */
public abstract  class StreamBuilder {


    private Properties streamConfig = new Properties();



    protected Util util;

    protected LinearRoadKafkaBenchmarkApplication.Context context;

    @Autowired
    public StreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        this.util = util;
        this.context = context;
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-"+this.context.getApplicationId());
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092");
        streamConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.2:2181");
    }

    public abstract void buildStream();

    public abstract Properties getStreamConfig();

    public Properties getBaseProperties() {
        return streamConfig;
    }
}
