package de.twiechert.linroad.kafka;

import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.core.StrTuple;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Arrays;
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

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(PositionReporter.class);
    /**
     * This callback processes tuples generated from the native c implementation.
     */
    public static class PositionTupleReceivedCallback implements DataDriverLibrary.TupleReceivedCallback {

        private final Producer<StrTuple, StrTuple> producer;

        public PositionTupleReceivedCallback(Producer<StrTuple, StrTuple> producer) {
            this.producer = producer;
        }

        public void invoke(String s) {
            String[] tuple = s.split(",");
            // in this implementation, the first three elements represent the key, as they have identifying character
            // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos) key -> Type = 0, Time, VID,  value -> Spd, XWay, Lane, Dir, Seg, Pos
            StrTuple key = new StrTuple(Arrays.copyOfRange(tuple, 0, 2));
            StrTuple value =  new StrTuple(Arrays.copyOfRange(tuple, 3, 8));
            logger.debug("Sending position report to kafka {}", s);
            producer.send(new ProducerRecord<>(TOPIC, key, value));
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
        Producer<StrTuple, StrTuple> producer = new KafkaProducer<>(properties);
        logger.debug("Initialized kafka producer to create position report");
        this.dataDriverLibrary.startProgram(filePath, new PositionTupleReceivedCallback(producer));
        logger.debug("finished position report");
        producer.close();
    }
}
