package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * Created by tafyun on 15.08.16.
 */
public class KStreamVoidKTableLeftJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {

    private final KTableValueGetterSupplier<K, V2> valueGetterSupplier;
    private final ValueJoiner<V1, V2, R> joiner;

    KStreamVoidKTableLeftJoin(KTableImpl<K, ?, V2> table, ValueJoiner<V1, V2, R> joiner) {
        this.valueGetterSupplier = table.valueGetterSupplier();
        this.joiner = joiner;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamVoidKTableLeftJoin.KStreamKTableLeftJoinProcessor(valueGetterSupplier.get());
    }

    private class KStreamKTableLeftJoinProcessor extends AbstractProcessor<K, V1> {

        private final KTableValueGetter<K, V2> valueGetter;

        public KStreamKTableLeftJoinProcessor(KTableValueGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
        }

        @Override
        public void process(K key, V1 value) {
            // if the key is null, we do not need proceed joining
            // the record with the table
            if (key != null) {
                context().forward(key, joiner.apply(value, valueGetter.get(key)));
            }
        }
    }

}
