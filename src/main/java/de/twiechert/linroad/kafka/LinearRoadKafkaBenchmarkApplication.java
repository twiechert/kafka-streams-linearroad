package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriver;
import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.feeder.DataFeeder;
import de.twiechert.linroad.kafka.stream.*;
import de.twiechert.linroad.kafka.stream.responses.Test;
import de.twiechert.linroad.kafka.stream.responses.Test2;
import net.moznion.random.string.RandomStringGenerator;
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
        private final DataFeeder positionReporter;
        private final LatestAverageVelocityStreamBuilder latestAverageVelocityStreamBuilder;
        private final AccidentDetectionStreamBuilder accidentDetectionStreamBuilder;
        private final AccidentNotificationStreamBuilder accidentNotificationStreamBuilder;

        private final NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder;
        private final CurrentTollStreamBuilder currentTollStreamBuilder;
        private final CurrentTollStreamBuilder2 currentTollStreamBuilder2;
        private final Test test;
        private final Test2 test2;


        @Autowired
        public BenchmarkRunner(Context context,
                               DataFeeder positionReporter,
                               LatestAverageVelocityStreamBuilder latestAverageVelocityStreamBuilder,
                               AccidentDetectionStreamBuilder accidentDetectionStreamBuilder,
                               NumberOfVehiclesStreamBuilder numberOfVehiclesStreamBuilder,
                               CurrentTollStreamBuilder currentTollStreamBuilder,
                               AccidentNotificationStreamBuilder accidentNotificationStreamBuilder,
                               CurrentTollStreamBuilder2 currentTollStreamBuilder2,
                               Test test,
                               Test2 test2) {
            this.context = context;
            this.positionReporter = positionReporter;
            this.latestAverageVelocityStreamBuilder = latestAverageVelocityStreamBuilder;
            this.accidentDetectionStreamBuilder = accidentDetectionStreamBuilder;
            this.numberOfVehiclesStreamBuilder = numberOfVehiclesStreamBuilder;
            this.currentTollStreamBuilder = currentTollStreamBuilder;
            this.currentTollStreamBuilder2 = currentTollStreamBuilder2;
            this.accidentNotificationStreamBuilder = accidentNotificationStreamBuilder;
            this.test2 = test2;

            this.test = test;
        }

        @Override
        public void run(String... var1) throws Exception {
            logger.debug("Starting benchmark");
            context.startExperiment();
            // a certain delay is required, because kafka streams will fail if reading from non-existent topic...
            logger.debug("Start feeding of tuples");
            positionReporter.startFeeding();

            test.startStream(true);
            test2.startStream(true);

            latestAverageVelocityStreamBuilder.startStream(false);
            numberOfVehiclesStreamBuilder.startStream(false);
            currentTollStreamBuilder2.startStream(false);

            //accidentDetectionStreamBuilder.startStream(true);
            accidentNotificationStreamBuilder.startStream(true);
           // currentTollStreamBuilder.startStream(true);

        }
    }

    @Component
    public static class Context {

        private final RandomStringGenerator generator = new RandomStringGenerator();


        @Value("${linearroad.data.path}")
        private String filePath;

        @Value("${linearroad.kafka.bootstrapservers}")
        private String bootstrapservers;



        private String applicationId = generator.generateByRegex("[0-9a-z]{3}");
        private DateTime benchmarkStartedAt = null; //  DateTime.now();

        public Context() {

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
        return new DataDriver(context.getFilePath());
    }


}
