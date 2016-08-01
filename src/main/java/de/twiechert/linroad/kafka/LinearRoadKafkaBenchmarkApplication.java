package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriver;
import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.feeder.DataFeeder;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.model.*;
import de.twiechert.linroad.kafka.stream.*;
import net.moznion.random.string.RandomStringGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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


        @Autowired
        private Context context;

        @Autowired
        private DataFeeder positionReporter;

        @Autowired
        private LatestAverageVelocityStreamBuilder latestAverageVelocityStreamBuilder;

        @Autowired
        private AccidentDetectionStreamBuilder accidentDetectionStreamBuilder;

        @Autowired
        private AccidentNotificationStreamBuilder accidentNotificationStreamBuilder;

        @Autowired
        private PositionReportStreamBuilder positionReportStreamBuilder;

        @Autowired
        private NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder;

        @Autowired
        private CurrentTollStreamBuilder currentTollStreamBuilder;

        @Autowired
        private TollNotificationStreamBuilder tollNotificationStreamBuilder;



        @Override
        public void run(String... var1) throws Exception {
            logger.debug("Starting benchmark");
            KStreamBuilder builder = new KStreamBuilder();

            context.startExperiment();
            // a certain delay is required, because kafka streams will fail if reading from non-existent topic...
            logger.debug("Start feeding of tuples");
            positionReporter.startFeeding();
            KStream<XwaySegmentDirection, PositionReport.Value> positionReportStream = positionReportStreamBuilder.getStream(builder);
            KStream<XwaySegmentDirection, NumberOfVehicles>  numberOfVehiclesStream = numberOfVehiclesStreamBuilder.getStream(positionReportStream);

            KStream<XwaySegmentDirection, AverageVelocity>  latestAverageVelocityStream = latestAverageVelocityStreamBuilder.getStream(positionReportStream);
            //latestAverageVelocityStream.print();

           // new LatestAverageVelocityStreamBuilder2().getStream(positionReportStream).print();

            KStream<XwaySegmentDirection, Long> accidentDetectionStream = accidentDetectionStreamBuilder.getStream(positionReportStream);
            accidentDetectionStream.print();

            KStream<Void, Quartet<Integer, Long, Long, Integer>> accidentNotificationStream = accidentNotificationStreamBuilder.getStream(positionReportStream, accidentDetectionStream);
            accidentNotificationStream.print();
            accidentNotificationStream.writeAsText("acc_notifications.csv", accidentNotificationStreamBuilder.getKeySerde(), accidentNotificationStreamBuilder.getValueSerde());

            accidentNotificationStream.to(accidentNotificationStreamBuilder.getKeySerde(), accidentNotificationStreamBuilder.getValueSerde(), accidentNotificationStreamBuilder.getOutputTopic());

            KStream<XwaySegmentDirection, CurrentToll> currentTollStream = currentTollStreamBuilder.getStream(latestAverageVelocityStream, numberOfVehiclesStream, accidentDetectionStream);
            //currentTollStream.print();

            currentTollStream.to(currentTollStreamBuilder.getKeySerde(), currentTollStreamBuilder.getValueSerde(), currentTollStreamBuilder.getOutputTopic());

            KStream<Void, TollNotification> stream = tollNotificationStreamBuilder.getStream(positionReportStream, currentTollStream);
            stream.print();

            KafkaStreams kafkaStreams = new KafkaStreams(builder, context.getStreamBaseConfig());
            kafkaStreams.start();
        }
    }

    @Component
    public static class Context {

        private final RandomStringGenerator generator = new RandomStringGenerator();

        private Properties streamBaseConfig = new Properties();

        private Properties producerBaseConfig = new Properties();


        @Value("${linearroad.data.path}")
        private String filePath;

        @Value("${linearroad.kafka.bootstrapservers}")
        private String bootstrapServers;

        @Value("${linearroad.zookeeper.server}")
        private String zookeeperServer;

        private String applicationId = generator.generateByRegex("[0-9a-z]{3}");
        private DateTime benchmarkStartedAt = null; //  DateTime.now();

        public Context() {
        }

        @PostConstruct
        private void initializeBaseConfig() {
            logger.debug("Configured kafka servers are {} and zookeeper {}", bootstrapServers, zookeeperServer);

            streamBaseConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-" + this.getApplicationId());
            streamBaseConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamBaseConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperServer);
            streamBaseConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReportHandler.TimeStampExtractor.class.getName());

            producerBaseConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerBaseConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerBaseConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
            producerBaseConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            producerBaseConfig.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            producerBaseConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        }

        public Properties getStreamBaseConfig() {
            return streamBaseConfig;
        }

        public Properties getProducerBaseConfig() {
            return producerBaseConfig;
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

        public String getApplicationId() {
            return applicationId;
        }


    }


    @Bean
    public DataDriverLibrary getDataDriverLibrary(DataDriver dataDriver) {
        return dataDriver.getLibrary();
    }


    @Bean
    public DataDriver getDataDriver(Context context) {
        logger.debug("Path to file is {}", context.getFilePath());
        return new DataDriver(context.getFilePath(), DataDriver.Architecture.X64_LINUX);
    }


}
