package de.twiechert.linroad.kafka.test;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.stream.AccidentDetectionStreamBuilder;
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

       // accidentDetectionStreamBuilder.buildStream();


    }
}
