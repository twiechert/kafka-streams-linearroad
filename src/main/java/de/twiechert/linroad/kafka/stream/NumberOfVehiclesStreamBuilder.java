package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.PositionReporter;
import de.twiechert.linroad.kafka.core.StreamBuilder;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Properties;

import static de.twiechert.linroad.kafka.core.Util.minuteOfReport;

/**
 * Created by tafyun on 31.05.16.
 */
@Component
public class NumberOfVehiclesStreamBuilder extends StreamBuilder{

    public static final String NUMBER_OF_CARS_IN_SEGMENT_EXPRESSWAY_DIRECTION_TOPIC = "NOV";




    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(NumberOfVehiclesStreamBuilder.class);

    @Autowired
    public NumberOfVehiclesStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util);
    }

    private TupleSerdes.TripletSerdes<Integer, Integer, Boolean> tripletKeySerdes =
            new TupleSerdes.TripletSerdes<>();

    private TupleSerdes.PairSerdes<Integer, Integer> valueSerdes =
            new TupleSerdes.PairSerdes<>();

    private Serde<Windowed<Triplet<Integer, Integer, Boolean>>> windowedKeySerde =
            Serdes.serdeFrom(new WindowedSerializer<>(tripletKeySerdes.serializer()), new WindowedDeserializer<>(tripletKeySerdes.deserializer()));

    @Override
    @Async
    public void buildStream() {
        logger.debug("Building stream to identify number of vehicles at expressway, segment and direction per minute.");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<Pair<Integer, Integer>, Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> source1 =
                builder.stream(PositionReporter.TOPIC);

        KStream<Windowed<Triplet<Integer, Integer, Boolean>>, Pair<Integer, Integer>> stream =
                // map to (expressway, segment, direction) (speed)
        source1.map((k,v) -> new KeyValue<>(new Triplet<>(v.getValue1(), v.getValue4(), v.getValue3()), new Pair<>(v.getValue0(), k.getValue0())  ))
                // calculate rolling average and minute the average related to (count of elements in window, current average, related minute for toll calculation)
                .aggregateByKey(() -> new Pair<>(0, 0),
                        (key, value, aggregat) ->  {
                    return new Pair<>(aggregat.getValue0()+1, Math.max(aggregat.getValue1(), minuteOfReport(value.getValue1()) ));
                }  , TimeWindows.of("window", 60)).toStream();

        stream.print();
        stream.to(windowedKeySerde, valueSerdes ,NUMBER_OF_CARS_IN_SEGMENT_EXPRESSWAY_DIRECTION_TOPIC);

        KafkaStreams kafkaStreams = new KafkaStreams(builder, this.getStreamConfig());
        kafkaStreams.start();
    }


    @Override
    public Properties getStreamConfig() {
        Properties properties = new Properties();
        properties.putAll(this.getBaseProperties());
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, NumberOfVehiclesStreamBuilder.KeySerde.class.getName() );
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, NumberOfVehiclesStreamBuilder.ValueSerde.class.getName() );
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PositionReporter.TimeStampExtractor.class.getName() );

        return properties;    }



    public static class KeySerde extends TupleSerdes.PairSerdes<Integer, Integer> {}
    public static class ValueSerde extends TupleSerdes.SextetSerdes<Integer, Integer, Integer, Boolean, Integer, Integer> {}


}
