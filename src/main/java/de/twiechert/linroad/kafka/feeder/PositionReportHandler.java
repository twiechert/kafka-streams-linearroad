package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.core.TupleTimestampExtrator;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.model.PositionReport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TimestampExtractor;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 21.07.16.
 * // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos) key -> Type = 0, Time, VID,  | value -> Spd, XWay, Lane, Dir, Seg, Pos*
 */
public class PositionReportHandler extends TupleHandler<PositionReport.Key, PositionReport.Value> {

    public static final String TOPIC = "POS";


    public PositionReportHandler() {
        super(0);
    }

    @Override
    protected PositionReport.Key transformKey(String[] tuple) {
        return new PositionReport.Key(pLng(tuple[1]), pInt(tuple[2]));
    }

    @Override
    protected PositionReport.Value transformValue(String[] tuple) {
        return new PositionReport.Value(pInt(tuple[3]), pInt(tuple[4]), pInt(tuple[5]), tuple[6].equals("0"), pInt(tuple[7]), pInt(tuple[8]));
    }

    @Override
    protected Class<? extends Serializer<PositionReport.Key>> getKeySerializerClass() {
        return PositionReport.KeySerializer.class;
    }

    @Override
    protected Class<? extends Serializer<PositionReport.Value>> getValueSerializerClass() {
        return PositionReport.ValueSerializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }


    public static class TimeStampExtractor implements TimestampExtractor {

        private TupleTimestampExtrator tupleTimestampExtrator = new TupleTimestampExtrator(TupleTimestampExtrator.KeyValue.Value, 0);

        @Override
        public long extract(ConsumerRecord<Object, Object> record) {
            if (record.key() instanceof PositionReport.Key)
                return ((PositionReport.Key) record.key()).getTime();
            else {
                return tupleTimestampExtrator.extract(record);
            }

        }
    }
}
