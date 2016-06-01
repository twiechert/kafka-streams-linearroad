package de.twiechert.linroad.kafka;

import de.twiechert.linroad.kafka.core.StrTuple;
import de.twiechert.linroad.kafka.core.StringArraySerde;
import de.twiechert.linroad.kafka.core.Util;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import static de.twiechert.linroad.kafka.PositionReporter.Key.*;
import static de.twiechert.linroad.kafka.PositionReporter.Value.*;

/**
 * Created by tafyun on 31.05.16.
 */
@Component
public class TollNotifier {

    private static final String CAR_IN_SEGMENT_EXPRESSWAY_DIRECTION_AT_MINUTE_TOPIC = "car_sgmt";

    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(TollNotifier.class);


    private Util util;

    private LinearRoadKafkaBenchmarkApplication.Context context;

    @Autowired
    public TollNotifier(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        this.util = util;
        this.context = context;
    }

    @Async
    public void startNotifying() {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<StrTuple, StrTuple>  source1 = builder.stream(new StringArraySerde(), new StringArraySerde(), PositionReporter.TOPIC);

        // build stream for cars, so that k
        logger.debug("Building cars stream");

        // consider that key -> Type = 0, Time, VID,  value -> Spd, XWay, Lane, Dir, Seg, Pos
        // remap so that all cars that drive on segment s on expressway x in direction d at minute m get the same key


        KStream<StrTuple, StrTuple> carInSgmentExpressWayDirectionAtMinuteStream =
                source1.map((key, value) -> new KeyValue<>(new StrTuple(value.get(Seg, XWay, Dir),  util.minuteOfReport(value.get(Time))), value));

        carInSgmentExpressWayDirectionAtMinuteStream.to(CAR_IN_SEGMENT_EXPRESSWAY_DIRECTION_AT_MINUTE_TOPIC);
        carInSgmentExpressWayDirectionAtMinuteStream.print();

        KafkaStreams kafkaStreams = new KafkaStreams(builder, context.getStreamConfig());
        kafkaStreams.start();
      //  source1.map((key, value) -> new KeyValue<>(key, value)).
            ///    print();

        // calculate cars count
       // source1.aggregateByKey()

                // for every tuple ...
        // group by key
        // within 30 seconds
        // create a toll notification...
    }
}
