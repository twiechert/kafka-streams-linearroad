package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriver;
import de.twiechert.linroad.jdriver.DataDriverLibrary;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.swing.text.Position;
import java.util.Properties;

/**
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@SpringBootApplication
@EnableAutoConfiguration
@EnableAsync
@Configuration
public class LinearRoadKafkaBenchmarkApplication {

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(LinearRoadKafkaBenchmarkApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(LinearRoadKafkaBenchmarkApplication.class, args);
    }


    @Component
    public static class BenchmarkRunner implements CommandLineRunner {

        private final Context context;
        private final PositionReporter positionReporter;
        private final TollNotifier tollNotifier;

        @Autowired
        public BenchmarkRunner(Context context, PositionReporter positionReporter, TollNotifier tollNotifier) {
            this.context = context;
            this.positionReporter = positionReporter;
            this.tollNotifier = tollNotifier;
        }

        @Override
        public void run(String... var1) throws Exception {
            logger.debug("Starting benchmark");
            context.startExperiment();
            logger.debug("Starting position report");
            positionReporter.startPositionReport();
            logger.debug("Starting toll notification");
            tollNotifier.startNotifying();
        }
    }

    @Component
    public static class Context {

        @Value("${linearroad.data.path}")
        private String filePath;

        @Value("${linearroad.kafka.bootstrapservers}")
        private String bootstrapservers;

        private Properties producerConfig = new Properties();
        private Properties streamConfig = new Properties();


        private DateTime benchmarkStartedAt = null; //  DateTime.now();

        public Context() {
            this.configure();
        }

        public int getCurrentRuntimeInSeconds() {
            return Seconds.secondsBetween(DateTime.now(), benchmarkStartedAt).getSeconds();
        }

        public void startExperiment() {
            this.benchmarkStartedAt = DateTime.now();
        }

        public String getFilePath() {
            return filePath;
        }

        public Properties getStreamConfig() {
            return streamConfig;
        }

        public Properties getProducerConfig() {
            return producerConfig;
        }

        private void configure() {
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092");
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            producerConfig.put(ProducerConfig.LINGER_MS_CONFIG,  1);
            producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "de.twiechert.linroad.kafka.core.StringArraySerializer");
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "de.twiechert.linroad.kafka.core.StringArraySerializer");

            streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark");
            streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092");
            streamConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.17.0.2:2181");
           // streamConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, "de.twiechert.linroad.kafka.core.StringArraySerde");
           // streamConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, "de.twiechert.linroad.kafka.core.StringArraySerde");

        }
    }


    @Bean
    public DataDriverLibrary getDataDriverLibrary(DataDriver dataDriver) {
        return dataDriver.getLibrary();
    }


    @Bean
    public DataDriver getDataDriver(Context context) {
        logger.debug("Path to file is {}", context.getFilePath());
        return new DataDriver(context.getFilePath());
    }


}
