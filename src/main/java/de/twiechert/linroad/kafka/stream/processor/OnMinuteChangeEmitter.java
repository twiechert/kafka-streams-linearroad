package de.twiechert.linroad.kafka.stream.processor;

import de.twiechert.linroad.kafka.model.TimedOnMinute;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams does not support windowing as known in Flink. In addition to the final value of a window-based aggregation, Kafka will emmit
 * also all intermediate aggregate values. This behaviour in a lot cases undesired.
 *
 * This processor filters a stream, such that the latest value of a certain key seen at minute m is emitted, if an element from minute m+1 (same key) is observed.
 * This is used for the stream of current tolls {@link de.twiechert.linroad.kafka.stream.CurrentTollStreamBuilder}, to get only one toll, the latest aggregate seen at minute m each.
 *
 *
 * This implementation does not work for sliding windows, because windows overlap and the logical applied here would lead to wrong results.
 */
public class OnMinuteChangeEmitter {
    private static final Logger logger = LoggerFactory
            .getLogger(OnMinuteChangeEmitter.class);

    private static final String PUNCTUATE_NAME = "KSTREAM-PUNCTUATE-";


    public static <K, V extends TimedOnMinute.TimedOnMinuteWithWindowEnd> KStream<K, V> getForWindowed(KStreamBuilder builder,
                                                                                                       KStream<K, V> sourceStream,
                                                                                                       Serde<K> keySerde,
                                                                                                       Serde<V> valueSerde,
                                                                                                       String storeName) {
        String name = builder.newName(PUNCTUATE_NAME);
        OnMinuteChangeWindowedEmmitProcessorSupplier<K, V> punctuateSupplier = new OnMinuteChangeWindowedEmmitProcessorSupplier<>(storeName);
        StateStoreSupplier stateStore = Stores.create(storeName)
                .withKeys(keySerde)
                .withValues(valueSerde)
                .inMemory()
                //.persistent()
                .build();

        builder.addProcessor(name, punctuateSupplier, sourceStream.getName());


        builder.addStateStore(stateStore, name);

        return new KStreamImpl<>(builder, name, sourceStream.getSourceNodes());
        //return new PunctuateProcessor<>(punctuate, storeName );
    }

    public static <K, V extends TimedOnMinute> KStream<K, V> get(KStreamBuilder builder,
                                                                 KStream<K, V> sourceStream,
                                                                 Serde<K> keySerde,
                                                                 Serde<V> valueSerde,
                                                                 String storeName) {
        String name = builder.newName(PUNCTUATE_NAME);
        OnMinuteChangeEmmitProcessorSupplier<K, V> punctuateSupplier = new OnMinuteChangeEmmitProcessorSupplier<>(storeName);
        StateStoreSupplier stateStore = Stores.create(storeName)
                .withKeys(keySerde)
                .withValues(valueSerde)
                .inMemory()
                //.persistent()
                .build();

        builder.addProcessor(name, punctuateSupplier, sourceStream.getName());


        builder.addStateStore(stateStore, name);

        return new KStreamImpl<>(builder, name, sourceStream.getSourceNodes());
        //return new PunctuateProcessor<>(punctuate, storeName );
    }


    public static class OnMinuteChangeWindowedEmmitProcessorSupplier<K, V extends TimedOnMinute.TimedOnMinuteWithWindowEnd> implements ProcessorSupplier<K, V> {


        private String storeName;

        public OnMinuteChangeWindowedEmmitProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }


        @Override
        public Processor<K, V> get() {
            return new OnMinuteChangeWindowedEmmitProcessor<>(storeName);
        }

    }

    private static class OnMinuteChangeWindowedEmmitProcessor<K, V extends TimedOnMinute.TimedOnMinuteWithWindowEnd> implements Processor<K, V> {
        private ProcessorContext context;
        private KeyValueStore<TimedKey<K>, V> kvStore;
        private String storeName;
        private static final int checkUpTo = 2;

        public OnMinuteChangeWindowedEmmitProcessor(String storename) {
            this.storeName = storename;

        }


        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            kvStore = (KeyValueStore) context.getStateStore(storeName);
        }

        @Override
        public void process(K key, V value) {
            kvStore.put(new TimedKey<>(key, value.getWindowEndMinute()), value);
            synchronized (this) {
                // it may be that at minte m there are no tuples => then tuple from m-1 is not emmited
                // --> thus do a lookup for 2 seconds
                for (int i = 1; i <= checkUpTo; i++) {
                    TimedKey<K> oldKey = new TimedKey<>(key, value.getMinute() - i);
                    V oldVal = kvStore.get(oldKey);
                    if (oldVal != null) {
                        context.forward(oldKey.getKey(), oldVal);
                        kvStore.delete(oldKey);
                    }
                }

            }

        }


        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
            kvStore.close();
        }
    }

    public static class OnMinuteChangeEmmitProcessorSupplier<K, V extends TimedOnMinute> implements ProcessorSupplier<K, V> {


        private String storeName;

        public OnMinuteChangeEmmitProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }


        @Override
        public Processor<K, V> get() {
            return new OnMinuteChangeEmmitProcessor<>(storeName);
        }

    }

    private static class OnMinuteChangeEmmitProcessor<K, V extends TimedOnMinute> implements Processor<K, V> {
        private ProcessorContext context;
        private KeyValueStore<TimedKey<K>, V> kvStore;
        private String storeName;
        private static final int checkUpTo = 2;

        public OnMinuteChangeEmmitProcessor(String storename) {
            this.storeName = storename;

        }


        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            kvStore = (KeyValueStore) context.getStateStore(storeName);
        }

        @Override
        public void process(K key, V value) {
            kvStore.put(new TimedKey<>(key, value.getMinute()), value);
            synchronized (this) {
                // it may be that at minte m there are no tuples => then tuple from m-1 is not emmited
                // --> thus do a lookup for 2 seconds
                for (int i = 1; i <= checkUpTo; i++) {
                    TimedKey<K> oldKey = new TimedKey<>(key, value.getMinute() - i);
                    V oldVal = kvStore.get(oldKey);
                    if (oldVal != null) {
                        context.forward(oldKey.getKey(), oldVal);
                        kvStore.delete(oldKey);
                    }
                }

            }

        }


        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {
            kvStore.close();
        }
    }


}