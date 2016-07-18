package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import static de.twiechert.linroad.kafka.core.Util.pInt;

import java.util.Properties;

/**
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class PositionReporter {

    public enum Key   {
        Type, Time, VID
    }

    public static enum Value   {
        Spd, XWay, Lane, Dir, Seg, Pos
    }

    public static final String TOPIC = "position_report";

    private static final Logger logger = LoggerFactory
            .getLogger(PositionReporter.class);
    /**
     * This callback processes tuples generated from the native c implementation.
     */
    public static class PositionTupleReceivedCallback implements DataDriverLibrary.TupleReceivedCallback {

        private final Producer< Pair<Integer, Integer> ,  Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> producer;

        public PositionTupleReceivedCallback(Producer< Pair<Integer, Integer>,  Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>>  producer) {
            this.producer = producer;
        }

        public void invoke(String s) {
            String[] tuple = s.split(",");

            // in this implementation, the first three elements represent the key, as they have identifying character
            // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos) key -> Type = 0, Time, VID,  | value -> Spd, XWay, Lane, Dir, Seg, Pos
            if(tuple[0].equals("0")) {
                Sextet<Integer, Integer, Integer, Boolean, Integer, Integer> value = new Sextet<>(pInt(tuple[3]), pInt(tuple[4]), pInt(tuple[5]), tuple[6].equals("0"), pInt(tuple[7]), pInt(tuple[8]));
                Pair<Integer, Integer> key = new Pair<>(pInt(tuple[1]), pInt(tuple[2]));
                //Pair<Integer, Integer> key = new Pair<>(22, 22);

                producer.send(new ProducerRecord<>(TOPIC, key, value));
            }

        }
    }


    private final DataDriverLibrary dataDriverLibrary;

    private final String filePath;

    private final Properties properties;

    @Autowired
    public PositionReporter(DataDriverLibrary dataDriverLibrary, LinearRoadKafkaBenchmarkApplication.Context context) {
        this.dataDriverLibrary = dataDriverLibrary;
        this.filePath = context.getFilePath();
        this.properties = context.getProducerConfig();
    }

    @Async
    public void startPositionReport() {
        Producer< Pair<Integer, Integer>,  Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> producer = new KafkaProducer<>(properties);

        logger.debug("Initialized kafka producer to create position report");
        this.dataDriverLibrary.startProgram(filePath, new PositionTupleReceivedCallback(producer));
        logger.debug("finished position report");
        producer.close();
    }

    public static class TimeStampExtractor implements TimestampExtractor {

        @Override
        public long extract(ConsumerRecord<Object, Object> record) {
                Pair<Integer, Integer>  castKey = ( Pair<Integer, Integer> ) record.key();
                return castKey.getValue0();

        }
    }

    public static class KeySerializer
            extends ByteArraySerde.BArraySerializer<Pair<Integer, Integer>>
            implements Serializer<Pair<Integer, Integer>> {
    }

    public static class ValueSerializer
            extends ByteArraySerde.BArraySerializer<Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>>
            implements Serializer<Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> {
    }


}
