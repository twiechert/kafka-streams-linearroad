package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriver;
import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.stream.AccidentDetectionStreamBuilder;
import de.twiechert.linroad.kafka.stream.LatestAverageVelocityStreamBuilder;
import de.twiechert.linroad.kafka.stream.NumberOfVehiclesStreamBuilder;
import net.moznion.random.string.RandomStringGenerator;
import org.apache.kafka.clients.producer.ProducerConfig;
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
        private final LatestAverageVelocityStreamBuilder carInSegStreamBuilder;
        private final AccidentDetectionStreamBuilder accidentDetectionStreamBuilder;
        private final NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder;

        @Autowired
        public BenchmarkRunner(Context context,
                               PositionReporter positionReporter,
                               LatestAverageVelocityStreamBuilder tollNotifier,
                               AccidentDetectionStreamBuilder accidentDetectionStreamBuilder,
                               NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder) {
            this.context = context;
            this.positionReporter = positionReporter;
            this.carInSegStreamBuilder = tollNotifier;
            this.accidentDetectionStreamBuilder = accidentDetectionStreamBuilder;
            this.numberOfVehiclesStreamBuilder = numberOfVehiclesStreamBuilder;
        }

        @Override
        public void run(String... var1) throws Exception {
            logger.debug("Starting benchmark");
            context.startExperiment();
            logger.debug("Starting position report");
            positionReporter.startPositionReport();
            // a certain delay is required, because kafka streams will fail if reading from non-existent topic...
            //Thread.sleep(3000L);

            numberOfVehiclesStreamBuilder.buildStream();

           // carInSegStreamBuilder.buildStream();
         //  accidentDetectionStreamBuilder.buildStream();
        }
    }

    @Component
    public static class Context {

        private final RandomStringGenerator generator = new RandomStringGenerator();


        @Value("${linearroad.data.path}")
        private String filePath;

        @Value("${linearroad.kafka.bootstrapservers}")
        private String bootstrapservers;

        private Properties producerConfig = new Properties();


        private String applicationId = generator.generateByRegex("[0-9a-z]{3}");
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

        public String getApplicationId() {
            return applicationId;
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
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PositionReporter.KeySerializer.class.getName());
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PositionReporter.ValueSerializer.class.getName());


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
