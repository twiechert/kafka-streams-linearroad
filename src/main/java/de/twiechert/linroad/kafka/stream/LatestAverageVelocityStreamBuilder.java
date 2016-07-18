package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.PositionReporter;
import de.twiechert.linroad.kafka.core.StreamBuilder;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.javatuples.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import static de.twiechert.linroad.kafka.core.Util.*;
import java.util.Properties;

/**
 * Created by tafyun on 31.05.16.
 */
@Component
public class LatestAverageVelocityStreamBuilder extends StreamBuilder{

    public static final String CAR_IN_SEGMENT_EXPRESSWAY_DIRECTION_AT_MINUTE_TOPIC = "car_sgmt";

    public static final String CAR_IN_SEGMENT_EXPRESSWAY_DIRECTION_AT_MINUTE_COUNT_TOPIC = "car_sgmt_cnt";

    public static final String CAR_IN_SEGMENT_EXPRESSWAY_DIRECTION_AT_MINUTE_AVG_SPEED_TOPIC = "car_sgmt_avg_speed";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(LatestAverageVelocityStreamBuilder.class);

    @Autowired
    public LatestAverageVelocityStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util);
    }

    private TupleSerdes.TripletSerdes<Integer, Integer, Boolean> tripletKeySerdes =
            new TupleSerdes.TripletSerdes<>();

    private TupleSerdes.TripletSerdes<Integer, Double, Integer> valueSerdes =
            new TupleSerdes.TripletSerdes<>();

    private Serde<Windowed<Triplet<Integer, Integer, Boolean>>> windowedKeySerde =
            Serdes.serdeFrom(new WindowedSerializer<>(tripletKeySerdes.serializer()), new WindowedDeserializer<>(tripletKeySerdes.deserializer()));

    @Override
    @Async
    public void buildStream() {
        logger.debug("Building stream to identify latest average velocity");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<Pair<Integer, Integer>, Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> source1 =
                builder.stream(PositionReporter.TOPIC);

        KStream<Windowed<Triplet<Integer, Integer, Boolean>>, Triplet<Integer, Double, Integer>> stream =
                // map to (expressway, segment, direction) (speed)
        source1.map((k,v) -> new KeyValue<>(new Triplet<>(v.getValue1(), v.getValue4(), v.getValue3()), new Pair<>(v.getValue0(), k.getValue0())  ))
                // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                .aggregateByKey(() -> new Triplet<>(0, 0d, 0),
                        (key, value, aggregat) ->  {
                         int n = aggregat.getValue0()+1;
                    return new Triplet<>(n,aggregat.getValue1()*(((double)n-1)/n)+(double)value.getValue0()/n, Math.max(aggregat.getValue2(), minuteOfReport(value.getValue1()) + 1 ));
                }  , TimeWindows.of("window", 5*60).advanceBy(60)).toStream();

        stream.print();
        stream.to(windowedKeySerde, valueSerdes ,"test");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, this.getStreamConfig());
        kafkaStreams.start();
    }


    @Override
    public Properties getStreamConfig() {
        Properties properties = new Properties();
        properties.putAll(this.getBaseProperties());
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, LatestAverageVelocityStreamBuilder.KeySerde.class.getName() );
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, LatestAverageVelocityStreamBuilder.ValueSerde.class.getName() );
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReporter.TimeStampExtractor.class.getName() );

        return properties;    }



    public static class KeySerde extends TupleSerdes.PairSerdes<Integer, Integer> {}
    public static class ValueSerde extends TupleSerdes.SextetSerdes<Integer, Integer, Integer, Boolean, Integer, Integer> {}


}
