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
 * Created by tafyun on 30.08.16.
 */
public class OnMinuteChangeEmitter {
    private static final Logger logger = LoggerFactory
            .getLogger(OnMinuteChangeEmitter.class);

    private static final String PUNCTUATE_NAME = "KSTREAM-PUNCTUATE-";


    public static <K, V extends TimedOnMinute> KStream<K, V> get(KStreamBuilder builder,
                                                                 KStream<K, V> sourceStream,
                                                                 Serde<K> keySerde,
                                                                 Serde<V> valueSerde,
                                                                 String storeName) {
        String name = builder.newName(PUNCTUATE_NAME);
        OnMiniteChangeEmmitProcessorSupplier<K, V> punctuateSupplier = new OnMiniteChangeEmmitProcessorSupplier<>(storeName);
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


    public static class OnMiniteChangeEmmitProcessorSupplier<K, V extends TimedOnMinute> implements ProcessorSupplier<K, V> {


        private String storeName;

        public OnMiniteChangeEmmitProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }


        @Override
        public Processor<K, V> get() {
            return new OnMiniteChangeEmmitProcessor<>(storeName);
        }

    }

    private static class OnMiniteChangeEmmitProcessor<K, V extends TimedOnMinute> implements Processor<K, V> {
        private ProcessorContext context;
        private KeyValueStore<TimedKey<K>, V> kvStore;
        private String storeName;

        public OnMiniteChangeEmmitProcessor(String storename) {
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
                TimedKey<K> oldKey = new TimedKey<>(key, value.getMinute() - 1);
                V oldVal = kvStore.get(oldKey);
                if (oldVal != null) {
                    context.forward(oldKey.getKey(), oldVal);
                    kvStore.delete(oldKey);
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