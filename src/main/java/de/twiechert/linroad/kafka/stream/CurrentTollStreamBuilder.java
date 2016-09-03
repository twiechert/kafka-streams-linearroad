package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.stream.processor.OnMinuteChangeEmitter;
import de.twiechert.linroad.kafka.model.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Quartet;
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


        //  KStream<XwaySegmentDirection, CurrentToll> xwaySegmentDirectionCurrentTollKStream =


        KStream<XwaySegmentDirection, CurrentTollIntermediate> joinedTollCalculationStream = latestAverageVelocityStream.through(new DefaultSerde<>(), new DefaultSerde<>(), context.topic("LAV_TOLL"))
                .join(numberOfVehiclesStream.through(new DefaultSerde<>(), new DefaultSerde<>(), context.topic("NOV_TOLL")),
                        (value1, value2) -> new CurrentTollIntermediate(value1.getMinute(), value1.getAverageSpeed(), value2.getNumberOfVehicles()),
                        JoinWindows.of(context.topic("LAV-NOV-WINDOW")), new DefaultSerde<>(), new DefaultSerde<>(), new DefaultSerde<>())

                .leftJoin(accidentDetectionStream,
                        (value1, value2) -> value1.setNoAccident(value2 == null),
                        JoinWindows.of(context.topic("LAV_NOV_ACC_WINDOW")),
                        new DefaultSerde<>(), new DefaultSerde<>());

        // only take latest aggs...
        return OnMinuteChangeEmitter.get(context.getBuilder(), joinedTollCalculationStream, new DefaultSerde<>(), new DefaultSerde<>(), "latest-toll")

                // if no toll, simply do not emmit -> the specification is in that regard not explicit enough
                .filter((k, v) -> v.hasNoAccident() && v.getAverageVelocity() < 40 && v.getNumberOfVehicles() > 50)
                // otherwise calculate toll
                .mapValues(CurrentTollIntermediate::calculateCurrentToll);
    }


    @Override
    public String getOutputTopic() {
        return TOPIC;
    }


    public static class CurrentTollIntermediate extends Quartet<Long, Double, Integer, Boolean> implements TimedOnMinute {


        public CurrentTollIntermediate(Long minute, Double averageVelocity, Integer numberOfVehicles) {
            super(minute, averageVelocity, numberOfVehicles, true);
        }

        public CurrentTollIntermediate setNoAccident(boolean noAccident) {
            this.setAt3(noAccident);
            return this;
        }

        public CurrentToll calculateCurrentToll() {
            return new CurrentToll(this.getMinute() + 1, 2 * Math.pow(this.getNumberOfVehicles() - 50, 2), this.getAverageVelocity());
        }


        public long getMinute() {
            return getValue0();
        }

        public double getAverageVelocity() {
            return getValue1();
        }

        public int getNumberOfVehicles() {
            return getValue2();
        }

        public boolean hasNoAccident() {
            return getValue3();
        }


    }

}
