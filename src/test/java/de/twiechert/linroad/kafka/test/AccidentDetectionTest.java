package de.twiechert.linroad.kafka.test;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.PositionReporter;
import de.twiechert.linroad.kafka.stream.AccidentDetectionStreamBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by tafyun on 10.07.16.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(LinearRoadKafkaBenchmarkApplication.class)
public class AccidentDetectionTest {



    private static final Logger logger = LoggerFactory
            .getLogger(AccidentDetectionTest.class);

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;

    @Autowired
    private AccidentDetectionStreamBuilder accidentDetectionStreamBuilder;

    @Test
    public void createAccidents() {

        Producer<StrTuple, StrTuple> producer = new KafkaProducer<>(context.getProducerConfig());

        // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos)
        // key -> Type = 0, Time, VID,  | value -> Spd, XWay, Lane, Dir, Seg, Pos
        logger.debug("Sending data to simulate an accident");
        for(int i = 1; i <= 26; i++) {
            producer.send(new ProducerRecord<>(PositionReporter.TOPIC, new StrTuple("0", 15*i+"", "v1"),
                    new StrTuple("22", "3", "2", "0", "33", "43")));
            producer.send(new ProducerRecord<>(PositionReporter.TOPIC, new StrTuple("0", 16*i+"", "v2"),
                    new StrTuple("22", "3", "2", "0", "33", "43")));
        }

        accidentDetectionStreamBuilder.buildStream();


    }
}
