package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriver;
import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.feeder.DataFeeder;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.feeder.historical.HistoricalDataFeeder;
import de.twiechert.linroad.kafka.model.*;
import de.twiechert.linroad.kafka.model.historical.*;
import de.twiechert.linroad.kafka.stream.*;
import de.twiechert.linroad.kafka.stream.historical.AccountBalanceRequestStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.AccountBalanceResponseStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.DailyExpenditureRequestStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.DailyExpenditureResponseStreamBuilder;
import de.twiechert.linroad.kafka.stream.historical.table.CurrentExpenditurePerVehicleTableBuilder;
import de.twiechert.linroad.kafka.stream.historical.table.TollHistoryTableBuilder;
import net.moznion.random.string.RandomStringGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
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
        private HistoricalDataFeeder historicalDataFeeder;

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

        @Autowired
        private TollHistoryTableBuilder tollHistoryTableBuilder;

        @Autowired
        private CurrentExpenditurePerVehicleTableBuilder currentExpenditurePerVehicleTableBuilder;

        @Autowired
        private AccountBalanceRequestStreamBuilder accountBalanceStreamBuilder;

        @Autowired
        private AccountBalanceResponseStreamBuilder accountBalanceResponseStreamBuilder;

        @Autowired
        private DailyExpenditureRequestStreamBuilder dailyExpenditureRequestStreamBuilder;

        @Autowired
        private DailyExpenditureResponseStreamBuilder dailyExpenditureResponseStreamBuilder;

        @Override
        public void run(String... var1) throws Exception {
            logger.debug("Using mode {}", context.getLinearRoadMode());
            KStreamBuilder builder = new KStreamBuilder();
            KTable<XwayVehicleDay, Double> tollHistoryTable = null;

            // historical info feeding
            if (!context.getLinearRoadMode().equals("no-historical-feed")) {
                tollHistoryTable = tollHistoryTableBuilder.getTable(builder);
            }

            // executing the actual benchmark
            if (!context.getLinearRoadMode().equals("no-benchmark")) {


                // a certain delay is required, because kafka streams will fail if reading from non-existent topic...

                logger.debug("Starting benchmark");

                tollHistoryTable = (tollHistoryTable == null) ? tollHistoryTableBuilder.getExistingTable(builder) : tollHistoryTable;


                /**
                 * Converting position reports to processable Kafka stream
                 */
                KStream<XwaySegmentDirection, PositionReport> positionReportStream = positionReportStreamBuilder.getStream(builder);
                /**
                 * Converting account balance request to processable Kafka stream
                 */
                KStream<AccountBalanceRequest, Void> accountBalanceRequestStream = accountBalanceStreamBuilder.getStream(builder);

                /**
                 * Converting daily expenditure request to processable Kafka stream
                 */
                KStream<DailyExpenditureRequest, Void> dailyExpenditureRequestStream = dailyExpenditureRequestStreamBuilder.getStream(builder);
                dailyExpenditureRequestStream.print();
                /**
                 * Building NOV stream
                 */
                KStream<XwaySegmentDirection, NumberOfVehicles> numberOfVehiclesStream = numberOfVehiclesStreamBuilder.getStream(positionReportStream);
                //numberOfVehiclesStream.print();

                /**
                 * Building LAV stream
                 */
                KStream<XwaySegmentDirection, AverageVelocity> latestAverageVelocityStream = latestAverageVelocityStreamBuilder.getStream(positionReportStream);
                //latestAverageVelocityStream.print();

                /**
                 * Building Accident detection stream
                 */
                KStream<XwaySegmentDirection, Long> accidentDetectionStream = accidentDetectionStreamBuilder.getStream(positionReportStream);
                accidentDetectionStream.print();

                /**
                 * Building Accident notification stream
                 */
                KStream<Void, AccidentNotification> accidentNotificationStream = accidentNotificationStreamBuilder.getStream(positionReportStream, accidentDetectionStream);
                accidentNotificationStream.print();
                accidentNotificationStream.writeAsText("output/" + accidentNotificationStreamBuilder.getOutputTopic() + ".csv", accidentNotificationStreamBuilder.getKeySerde(), accidentNotificationStreamBuilder.getValueSerde());
                accidentNotificationStream.to(accidentNotificationStreamBuilder.getKeySerde(), accidentNotificationStreamBuilder.getValueSerde(), accidentNotificationStreamBuilder.getOutputTopic());

                /**
                 * Building current toll per Xway-Segmen-Directon tuple stream
                 */
                KStream<XwaySegmentDirection, CurrentToll> currentTollStream = currentTollStreamBuilder.getStream(latestAverageVelocityStream, numberOfVehiclesStream, accidentDetectionStream);
                currentTollStream
                        .filter((k, v) -> v.getToll() > 0)
                        .print();
                //currentTollStream.to(currentTollStreamBuilder.getKeySerde(), currentTollStreamBuilder.getValueSerde(), currentTollStreamBuilder.getOutputTopic());

                /**
                 * Building stream to notify driver about tolls
                 */
                KStream<Void, TollNotification> tollNotificationStream = tollNotificationStreamBuilder.getStream(positionReportStream, currentTollStream);
                tollNotificationStream.writeAsText("output/" + tollNotificationStreamBuilder.getOutputTopic() + ".csv", tollNotificationStreamBuilder.getKeySerde(), tollNotificationStreamBuilder.getValueSerde());



                /**
                 * Creating tables to retain the latest state about tolls
                 * (a) recent toll per vehicle
                 * (b) toll per vehicle, per day, per expressway
                 */
                // this table may be derived from the above (how to realize in Kafka streams?)
                KTable<Integer, ExpenditureAt> tollPerVehicleTable = currentExpenditurePerVehicleTableBuilder.getStream(tollNotificationStream);

                /**
                 * Building stream to answer account balance requests
                 */
                KStream<Void, AccountBalanceResponse> accountBalanceResponseStream = accountBalanceResponseStreamBuilder.getStream(accountBalanceRequestStream, tollPerVehicleTable);
                accountBalanceResponseStream.writeAsText("output/" + accountBalanceResponseStreamBuilder.getOutputTopic() + ".csv", accountBalanceResponseStreamBuilder.getKeySerde(), accountBalanceResponseStreamBuilder.getValueSerde());
                // accountBalanceResponseStream.print();

                /**
                 * Building stream to answer daily expenditure requests
                 */
                KStream<Void, DailyExpenditureResponse> dailyExpenditureResponseStream = dailyExpenditureResponseStreamBuilder.getStream(dailyExpenditureRequestStream, tollHistoryTable);
                dailyExpenditureResponseStream.writeAsText("output/" + dailyExpenditureResponseStreamBuilder.getOutputTopic() + ".csv", dailyExpenditureResponseStreamBuilder.getKeySerde(), dailyExpenditureResponseStreamBuilder.getValueSerde());
                dailyExpenditureResponseStream.print();


            }


            KafkaStreams kafkaStreams = new KafkaStreams(builder, context.getStreamBaseConfig());
            kafkaStreams.start();

            if (!context.getLinearRoadMode().equals("no-historical-feed")) {
                logger.debug("Start feeding with historical data");

                // must be synchronous -> actual benchmark must not begin before
                historicalDataFeeder.startFeeding();
            }

            if (!context.getLinearRoadMode().equals("no-benchmark")) {
                logger.debug("Start feeding of tuples");
                positionReporter.startFeeding();
            }
        }
    }

    @Component
    public static class Context {

        private final RandomStringGenerator generator = new RandomStringGenerator();

        private final Properties streamBaseConfig = new Properties();

        private final Properties producerBaseConfig = new Properties();


        @Value("${linearroad.hisotical.data.path}")
        private String historicalFilePath;

        @Value("${linearroad.data.path}")
        private String filePath;

        @Value("${linearroad.kafka.bootstrapservers}")
        private String bootstrapServers;

        @Value("${linearroad.zookeeper.server}")
        private String zookeeperServer;

        @Value("${linearroad.mode}")
        private String linearRoadMode;


        private final String applicationId = "1";//generator.generateByRegex("[0-9a-z]{3}");
        private DateTime benchmarkStartedAt = DateTime.now();

        public Context() {
        }

        @PostConstruct
        private void initializeBaseConfig() {
            logger.debug("Configured kafka servers are {} and zookeeper {}", bootstrapServers, zookeeperServer);

            streamBaseConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "linearroad-benchmark-" + this.getApplicationId());
            streamBaseConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamBaseConfig.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperServer);
            streamBaseConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReportHandler.TimeStampExtractor.class.getName());
            //   streamBaseConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 0);

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

        public long getCurrentRuntimeInSeconds() {
            return Seconds.secondsBetween(benchmarkStartedAt, DateTime.now()).getSeconds();
        }

        public void markAsStarted() {
            this.benchmarkStartedAt = DateTime.now();
        }

        public String getFilePath() {
            return filePath;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getHistoricalFilePath() {
            return historicalFilePath;
        }

        public String getLinearRoadMode() {
            return linearRoadMode;
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
