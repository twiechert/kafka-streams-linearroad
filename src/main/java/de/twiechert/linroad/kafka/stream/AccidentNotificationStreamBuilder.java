package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.FallbackTimestampExtractor;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.model.AccidentNotification;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class notifies drivers about occured accidents if they are close-by according to the LR requirements.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class AccidentNotificationStreamBuilder extends StreamBuilder<Void, AccidentNotification> {

    private static final String TOPIC = "ACC_NOT";

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(AccidentNotificationStreamBuilder.class);


    @Autowired
    public AccidentNotificationStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util, new Void.Serde(), new AccidentNotification.Serde());
    }

    public KStream<Void, AccidentNotification> getStream(KStream<XwaySegmentDirection, PositionReport.Value> positionReports,
                                                         KStream<XwaySegmentDirection, Long> accidentReports) {
        logger.debug("Building stream to notify drivers about accidents");

        /**
         * The trigger for an accident notification is a position report
         * that identifies a vehicle entering a segment 0 to 4 segments upstream of some accident location,
         * but only if q was emitted no earlier than theminute following theminutewhen the accident occurred, and no later than the minute the accident is
         */

        // IMPORTANT for joining: , but only if q (position report) was emitted
        // **no** earlier than the minute following them inutew hen the accident occurred.
        // i.e. the accident detection must be "before" up to one second
       return  accidentReports.through(new XwaySegmentDirection.Serde(), new Serdes.LongSerde(), "ACC_DET_NOT")
               .join(positionReports, (value1, value2) -> new Pair<>(value1, value1), JoinWindows.of("ACC-NOT-WINDOW").before(60),
                        new XwaySegmentDirection.Serde(), new Serdes.LongSerde(), new PositionReport.ValueSerde())
               .map((k, v) -> new KeyValue<>(new Void(), new AccidentNotification(v.getValue0(), context.getCurrentRuntimeInSeconds(), k.getSeg())));

    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }

}
