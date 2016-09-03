package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This stream generates a pair of the current toll and LAV per XwaySegmentDirection tuple
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 *
 */
@Component
public class CurrentTollStreamBuilder extends StreamBuilder<XwaySegmentDirection, CurrentToll> {

    public static final String TOPIC = "CURRENT_TOLL";


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(CurrentTollStreamBuilder.class);

    @Autowired
    public CurrentTollStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        super(context);
    }


    public KStream<XwaySegmentDirection, CurrentToll> getStream(KStream<XwaySegmentDirection, AverageVelocity> latestAverageVelocityStream,
                                                                KStream<XwaySegmentDirection, NumberOfVehicles> numberOfVehiclesStream,
                                                                KStream<XwaySegmentDirection, Long> accidentDetectionStream) {
        logger.debug("Building stream to calculate the current toll on expressway, segent, direction..");


        return latestAverageVelocityStream.through(new DefaultSerde<>(), new DefaultSerde<>(), context.topic("LAV_TOLL"))
                .join(numberOfVehiclesStream.through(new DefaultSerde<>(), new DefaultSerde<>(), context.topic("NOV_TOLL")),
                        (value1, value2) -> new Triplet<>(value1.getValue0(), value1.getValue1(), value2.getValue1()),
                        JoinWindows.of(context.topic("LAV-NOV-WINDOW")), new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>())

                .leftJoin(accidentDetectionStream,
                        (value1, value2) -> new Quartet<>(value1.getValue0(), value1.getValue1(), value1.getValue2(), value2 == null),
                        JoinWindows.of(context.topic("LAV_NOV_ACC_WINDOW")),
                        new DefaultSerde<>(), new DefaultSerde<>())
                .mapValues(v -> {

                    // logger.debug("Minute {} Accident {}, Speed {}, NOV {}",v.getValue0(),  v.getValue3(), v.getValue1(), v.getValue2());
                    // accident --> no toll
                    if (v.getValue3() && v.getValue1() < 40 && v.getValue2() > 50)
                        return new CurrentToll(Util.minuteOfReport(v.getValue0()) + 1, 2 * Math.pow(v.getValue2() - 50, 2), v.getValue1());
                    else
                        return new CurrentToll(Util.minuteOfReport(v.getValue0()) + 1, 0d, v.getValue1());

                });

    }


    @Override
    public String getOutputTopic() {
        return TOPIC;
    }


}
