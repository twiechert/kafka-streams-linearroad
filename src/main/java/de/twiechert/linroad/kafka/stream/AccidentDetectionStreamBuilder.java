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
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Sextet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Created by tafyun on 02.06.16.
 */
@Component
public class AccidentDetectionStreamBuilder extends StreamBuilder {

    private static final String ACCIDENT_DETECTION_TOPIC = "acc_topic";

    private static final Logger logger = LoggerFactory
            .getLogger(AccidentDetectionStreamBuilder.class);


    private TupleSerdes.QuintetSerdes<Integer, Integer, Boolean, Integer, Integer> quintetKeySerdes =
            new TupleSerdes.QuintetSerdes<>();


    private TupleSerdes.QuartetSerdes<Integer, Integer, Boolean, Integer> quartetValueSerdes =
            new TupleSerdes.QuartetSerdes<>();

    private Serde<Windowed<Quintet<Integer, Integer, Boolean, Integer, Integer>>> windowedKeySerde =
            Serdes.serdeFrom(new WindowedSerializer<>(quintetKeySerdes.serializer()), new WindowedDeserializer<>(quintetKeySerdes.deserializer()));

    @Autowired
    public AccidentDetectionStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util);
    }

    @Override
    @Async
    public void buildStream() {
        logger.debug("Building stream to identify accidents");
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Pair<Integer, Integer>,  Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> source1 =
                builder.stream(new TupleSerdes.PairSerdes<>(),
                               new TupleSerdes.SextetSerdes<>(), PositionReporter.TOPIC);

        KStream<Windowed<Quintet<Integer, Integer, Boolean, Integer, Integer>>, Quartet<Integer, Integer, Boolean, Integer>> crashedCarsStream =  source1.map((key, value) -> new KeyValue<>(
                // Xway, Lane
                new Quintet<>(value.getValue1(), value.getValue2(), value.getValue3(), value.getValue4(), value.getValue5()),
                // VID , minute
                new Pair<>(key.getValue1(), Util.minuteOfReport(key.getValue0()))))
                // current time to use | if more than one vehicle in window | current count of position reports in window

                        .aggregateByKey(() -> new Quartet<>(0, -1, false, 0),
                                (key, value, aggregat) ->  {

                                    int time = value.getValue1() > aggregat.getValue0() ? value.getValue1() : aggregat.getValue0();
                                    // indicates if there are multiple cars in the considered window
                                    boolean multiple =       aggregat.getValue2() ||
                                                            (aggregat.getValue1() != -1 && (!aggregat.getValue1().equals(key.getValue0()))) ;
                                    return new Quartet<>(time, key.getValue0(), multiple, aggregat.getValue3()+1);
                                }
                                , TimeWindows.of("window", 4*30).advanceBy(30)).toStream();


    //   crashedCarsStream.map((key, value) -> new KeyValue<>(new Windowed<>(new StrTuple(key.key().first(5)), key.window()), value))
      //                 .countByKey(windowedSerde, "cars_same_position")
         //              .filter((key, value) -> value >= 2).toStream();


        crashedCarsStream.to(windowedKeySerde, quartetValueSerdes, ACCIDENT_DETECTION_TOPIC);
        crashedCarsStream.print();

        KafkaStreams kafkaStreams = new KafkaStreams(builder, this.getStreamConfig());
        kafkaStreams.start();
    }

    @Override
    public Properties getStreamConfig() {
        Properties properties = new Properties();
        properties.putAll(this.getBaseProperties());
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, AccidentDetectionStreamBuilder.KeySerde.class.getName() );
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, AccidentDetectionStreamBuilder.ValueSerde.class.getName() );
        return properties;
    }

    public static class KeySerde extends TupleSerdes.PairSerdes<Integer, Integer> {}

    public static class ValueSerde extends TupleSerdes.SextetSerdes<Integer, Integer, Integer, Boolean, Integer, Integer> {}
}
