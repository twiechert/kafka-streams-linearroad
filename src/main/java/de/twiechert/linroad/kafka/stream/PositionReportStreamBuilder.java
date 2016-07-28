package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import de.twiechert.linroad.kafka.model.PositionReport;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.javatuples.Pair;
import org.javatuples.Sextet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 28.07.16.
 */
@Component
public class PositionReportStreamBuilder {

    public PositionReportStreamBuilder() {
    }

    public KStream<PositionReport.Key, PositionReport.Value> getStream(KStreamBuilder builder) {
         return builder.stream(new PositionReport.KeySerde(),
                 new PositionReport.ValueSerde(), PositionReportHandler.TOPIC);
    }
}
