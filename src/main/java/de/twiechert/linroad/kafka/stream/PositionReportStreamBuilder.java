package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.model.PositionReport;
import de.twiechert.linroad.kafka.model.XwaySegmentDirection;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 28.07.16.
 */
@Component
public class PositionReportStreamBuilder {

    public KStream<XwaySegmentDirection, PositionReport> getStream(KStreamBuilder builder) {
        return builder.stream(new DefaultSerde<>(), new DefaultSerde<>(), PositionReportHandler.TOPIC);
    }
}
