package de.twiechert.linroad.kafka.core.feeder;

import de.twiechert.linroad.kafka.core.TupleTimestampExtrator;
import de.twiechert.linroad.kafka.core.serde.ByteArraySerde;
import de.twiechert.linroad.kafka.core.serde.TupleSerdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.javatuples.Pair;
import org.javatuples.Sextet;

import static de.twiechert.linroad.kafka.core.Util.pInt;
import static de.twiechert.linroad.kafka.core.Util.pLng;

/**
 * Created by tafyun on 21.07.16.
 * // (Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos) key -> Type = 0, Time, VID,  | value -> Spd, XWay, Lane, Dir, Seg, Pos*
 */
public class PositionReportHandler extends TupleHandler<Pair<Long, Integer>, Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> {

    public static final String TOPIC = "POS";


    public PositionReportHandler() {
        super(0);
    }

    @Override
    protected Pair<Long, Integer> transformKey(String[] tuple) {
        return  new Pair<>(pLng(tuple[1]), pInt(tuple[2]));
    }

    @Override
    protected Sextet<Integer, Integer, Integer, Boolean, Integer, Integer> transformValue(String[] tuple) {
        return new Sextet<>(pInt(tuple[3]), pInt(tuple[4]), pInt(tuple[5]), tuple[6].equals("0"), pInt(tuple[7]), pInt(tuple[8]));
    }

    @Override
    protected Class<? extends Serializer<Pair<Long, Integer>>> getKeySerializerClass() {
        return KeySerializer.class;
    }

    @Override
    protected Class<? extends Serializer< Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>>> getValueSerializerClass() {
        return ValueSerializer.class;
    }

    @Override
    protected String getTopic() {
        return TOPIC;
    }

    public static class KeySerializer
            extends ByteArraySerde.BArraySerializer<Pair<Long, Integer>>
            implements Serializer<Pair<Long, Integer>> {
    }

    public static class ValueSerializer
            extends ByteArraySerde.BArraySerializer<Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>>
            implements Serializer<Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>> {
    }

    public static class PositionReportKeySerde extends TupleSerdes.PairSerdes<Long, Integer> {}
    public static class PositionReportValueSerde extends TupleSerdes.SextetSerdes<Integer, Integer, Integer, Boolean, Integer, Integer> {}

    public static class TimeStampExtractor extends TupleTimestampExtrator implements TimestampExtractor {
        public TimeStampExtractor() {
            super(KeyValue.Key, 0);
        }
    }
}
