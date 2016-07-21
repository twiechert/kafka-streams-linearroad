package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.javatuples.Pair;
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
public class CurrentTollStreamBuilder extends StreamBuilder<Triplet<Integer, Integer, Boolean>, Pair<Integer, Double>>{

    public static final String TOPIC = "CURRENT_TOLL";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(CurrentTollStreamBuilder.class);

    @Autowired
    public CurrentTollStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context,
              util,
              new TupleSerdes.TripletSerdes<>() ,
              new TupleSerdes.PairSerdes<>());
    }


    @Override
    protected KStream<Triplet<Integer, Integer, Boolean>, Pair<Integer, Double>> getStream(KStreamBuilder builder) {
        logger.debug("Building stream to calculate the current toll on expressway, segent, direction..");

        KStream<Triplet<Integer, Integer, Boolean>, Pair<Long, Integer>> numberOfVehicleStream =
                builder.stream(new LatestAverageVelocityStreamBuilder.KeySerde(), new NumberOfVehiclesStreamBuilder.ValueSerde(), NumberOfVehiclesStreamBuilder.TOPIC);

        KStream<Triplet<Integer, Integer, Boolean>, Pair<Long, Double>> latestAverageVelocityStream =
                builder.stream(new LatestAverageVelocityStreamBuilder.KeySerde(), new LatestAverageVelocityStreamBuilder.ValueSerde(), LatestAverageVelocityStreamBuilder.TOPIC);

        KStream<Triplet<Integer, Integer, Boolean>, Pair<Integer, Double>> tollStream =
                latestAverageVelocityStream.join(numberOfVehicleStream, (value1, value2) -> new Pair<>(0, 1d), JoinWindows.of("LAV_NOV").before(1));

        return tollStream;
    }

    @Override
    public Properties getStreamConfig() {
        Properties properties = new Properties();
        properties.putAll(this.getBaseProperties());
        // very important
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LatestAverageVelocityStreamBuilder.TimeStampExtractor.class.getName() );
        return properties;
    }

    @Override
    protected String getOutputTopic() {
        return TOPIC;
    }




}
