package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Created by tafyun on 31.05.16.
 */
@Component
public class CurrentTollStreamBuilder extends TableAndStreamBuilder.StreamBuilder<Triplet<Integer, Integer, Boolean>, Quartet<Long, Double, Integer, Long>> {

    public static final String TOPIC = "CURRENT_TOLL";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(CurrentTollStreamBuilder.class);

    @Autowired
    public CurrentTollStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context,
                util,
                new TupleSerdes.TripletSerdes<>(),
                new TupleSerdes.QuartetSerdes<>());
    }


    @Override
    protected KStream<Triplet<Integer, Integer, Boolean>, Quartet<Long, Double, Integer, Long>> getStream(KStreamBuilder builder) {
        logger.debug("Building stream to calculate the current toll on expressway, segent, direction..");

        KStream<Triplet<Integer, Integer, Boolean>, Pair<Long, Integer>> numberOfVehicleStream =
                builder.stream(new LatestAverageVelocityStreamBuilder.KeySerde(), new NumberOfVehiclesStreamBuilder.ValueSerde(), NumberOfVehiclesStreamBuilder.TOPIC);

        KStream<Triplet<Integer, Integer, Boolean>, Pair<Long, Double>> latestAverageVelocityStream =
                builder.stream(new LatestAverageVelocityStreamBuilder.KeySerde(), new LatestAverageVelocityStreamBuilder.ValueSerde(), LatestAverageVelocityStreamBuilder.TOPIC);

        KStream<Triplet<Integer, Integer, Boolean>, Long> accidentDetectionStreamBuilder =
                builder.stream(new AccidentDetectionStreamBuilder.KeySerde(), new AccidentDetectionStreamBuilder.ValueSerde(), AccidentDetectionStreamBuilder.TOPIC);


        return latestAverageVelocityStream.join(numberOfVehicleStream,
                (value1, value2) -> new Triplet<>(value1.getValue0(), value1.getValue1(), value2.getValue1()), JoinWindows.of("LAV_NOV").before(2))
                .leftJoin(accidentDetectionStreamBuilder,
                        (value1, value2) -> new Quartet<>(value1.getValue0(), value1.getValue1(), value1.getValue2(), value2), JoinWindows.of("LAV_NOV_ACC").before(1));



    }

    @Override
    public Properties getStreamConfig() {
        Properties properties = new Properties();
        properties.putAll(this.getBaseProperties());
        // very important
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LatestAverageVelocityStreamBuilder.TimeStampExtractor.class.getName());
        return properties;
    }

    @Override
    protected String getOutputTopic() {
        return TOPIC;
    }


}
