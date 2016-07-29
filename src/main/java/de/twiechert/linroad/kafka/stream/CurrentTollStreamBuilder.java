package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.model.AverageVelocity;
import de.twiechert.linroad.kafka.model.NumberOfVehicles;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 31.05.16.
 */
@Component
public class CurrentTollStreamBuilder extends StreamBuilder<XwaySegmentDirection, Double> {

    public static final String TOPIC = "CURRENT_TOLL";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(CurrentTollStreamBuilder.class);

    @Autowired
    public CurrentTollStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context,
                util,
                new XwaySegmentDirection.Serde(),
                new Serdes.DoubleSerde());
    }


    public KStream<XwaySegmentDirection, Double> getStream(KStream<XwaySegmentDirection, AverageVelocity> latestAverageVelocityStream,
                                                           KStream<XwaySegmentDirection, NumberOfVehicles> numberOfVehiclesStream,
                                                           KStream<XwaySegmentDirection, Long> accidentDetectionStreamBuilder) {
        logger.debug("Building stream to calculate the current toll on expressway, segent, direction..");


        return latestAverageVelocityStream.through(new XwaySegmentDirection.Serde(), new AverageVelocity.Serde(), "LAV_TOLL")
                .join(numberOfVehiclesStream.through(new XwaySegmentDirection.Serde(), new NumberOfVehicles.Serde(), "NOV_TOLL"),
                        (value1, value2) -> new Triplet<>(value1.getValue0(), value1.getValue1(), value2.getValue1()),
                        JoinWindows.of("LAV-NOV-WINDOW").before(2), new XwaySegmentDirection.Serde(), new AverageVelocity.Serde(), new NumberOfVehicles.Serde())

                .leftJoin(accidentDetectionStreamBuilder.through(new XwaySegmentDirection.Serde(), new Serdes.LongSerde(), "ACC_TOLL"),
                        (value1, value2) -> new Quartet<>(value1.getValue0(), value1.getValue1(), value1.getValue2(), value2 != null),
                        JoinWindows.of("LAV-NOV-ACC-WINDOW").before(1),
                        new XwaySegmentDirection.Serde(), new Serdes.LongSerde())
                .mapValues(v -> {
                    // accident --> no toll
                    if (v.getValue3() || v.getValue1() >= 40 || v.getValue2() <= 50)
                        return 0d;
                    else
                        return 2 * Math.pow(v.getValue2() - 50, 2);

                }).filter((k,v) -> v > 0);

    }


    @Override
    public String getOutputTopic() {
        return TOPIC;
    }


}
