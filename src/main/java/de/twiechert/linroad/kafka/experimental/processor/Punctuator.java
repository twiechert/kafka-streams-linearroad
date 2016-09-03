package de.twiechert.linroad.kafka.experimental.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tafyun on 30.08.16.
 */
public class Punctuator {
    private static final Logger logger = LoggerFactory
            .getLogger(Punctuator.class);

    private static final String PUNCTUATE_NAME = "KSTREAM-PUNCTUATE-";
    private static final String PUNCTUATE_SLIDE_NAME = "KSTREAM-SLIDE-PUNCTUATE-";

    public static <K, V> KStream<K, Pair<Long, V>> getForSliding(KStreamBuilder builder,
                                                                 KStream<Windowed<K>, V> sourceStream,
                                                                 TimeWindows timeWindows,
                                                                 Serde<K> keySerde,
                                                                 Serde<V> valueSerde,
                                                                 String storeName) {
        ProcessorSupplier<Windowed<K>, V> punctuateSupplier = new WindowedPunctuateProcessorSupplier<>(storeName, timeWindows);
        return getForSlidingWithProcessor(builder, sourceStream, timeWindows, keySerde, valueSerde, storeName, punctuateSupplier);

    }

    public static <K, V> KStream<K, Pair<Long, V>> getForLav(KStreamBuilder builder,
                                                             KStream<Windowed<K>, V> sourceStream,
                                                             Serde<K> keySerde,
                                                             Serde<V> valueSerde,
                                                             String storeName) {
        ProcessorSupplier<Windowed<K>, V> punctuateSupplier = new LavPunctuateProcessorSupplier<>(storeName);
        return getForSlidingWithProcessor(builder, sourceStream, null, keySerde, valueSerde, storeName, punctuateSupplier);

    }


    private static <K, V> KStream<K, Pair<Long, V>> getForSlidingWithProcessor(KStreamBuilder builder,
                                                                               KStream<Windowed<K>, V> sourceStream,
                                                                               TimeWindows timeWindows,
                                                                               Serde<K> keySerde,
                                                                               Serde<V> valueSerde,
                                                                               String storeName,
                                                                               ProcessorSupplier<Windowed<K>, V> punctuateSupplier) {
        String name = builder.newName(PUNCTUATE_SLIDE_NAME);
        StateStoreSupplier stateStore = Stores.create(storeName)
                .withKeys(keySerde)
                .withValues(valueSerde)
                .inMemory()
                //.persistent()
                .build();

        builder.addProcessor(name, punctuateSupplier, sourceStream.getName());


        builder.addStateStore(stateStore, name);

        return new KStreamImpl<>(builder, name, sourceStream.getSourceNodes());
    }

    public static <K, V> KStream<K, V> get(KStreamBuilder builder,
                                           KStream<K, V> sourceStream,
                                           long punctuate,
                                           Serde<K> keySerde,
                                           Serde<V> valueSerde,
                                           String storeName) {
        String name = builder.newName(PUNCTUATE_NAME);
        PunctuateProcessorSupplier<K, V> punctuateSupplier = new PunctuateProcessorSupplier<>(punctuate, storeName);
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


    public static class PunctuateProcessorSupplier<K, V> implements ProcessorSupplier<K, V> {


        private long punctuate;
        private String storeName;

        public PunctuateProcessorSupplier(long punctuate, String storeName) {
            this.punctuate = punctuate;
            this.storeName = storeName;
        }


        @Override
        public Processor<K, V> get() {
            return new PunctuateProcessor<>(punctuate, storeName);
        }

    }

    private static class PunctuateProcessor<K, V> implements Processor<K, V> {
        private ProcessorContext context;
        private long punctuate;
        private KeyValueStore<K, V> kvStore;
        private String storeName;

        public PunctuateProcessor(long punctuate, String storename) {
            this.punctuate = punctuate;
            this.storeName = storename;

        }


        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.context.schedule(this.punctuate, this.punctuate);
            kvStore = (KeyValueStore) context.getStateStore(storeName);

        }

        @Override
        public void process(K key, V value) {
            kvStore.put(key, value);
        }

        @Override
        public void punctuate(long timestamp) {
            logger.debug("Punctuating.");
            KeyValueIterator<K, V> iter = this.kvStore.all();
            while (iter.hasNext()) {
                KeyValue<K, V> entry = iter.next();
                context.forward(entry.key, entry.value);
                //  this.kvStore.delete(entry.key);
            }
            iter.close();
            context.commit();

        }

        @Override
        public void close() {
            kvStore.close();
        }
    }

    public static class WindowedPunctuateProcessorSupplier<K, V> implements ProcessorSupplier<Windowed<K>, V> {


        private String storeName;
        private TimeWindows windows;

        public WindowedPunctuateProcessorSupplier(String storeName, TimeWindows windows) {
            this.storeName = storeName;
            this.windows = windows;
        }


        @Override
        public Processor<Windowed<K>, V> get() {
            return new SlidingWindowedPunctuateProcessor<>(windows, storeName);
        }

    }

    public static class LavPunctuateProcessorSupplier<K, V> implements ProcessorSupplier<Windowed<K>, V> {


        private String storeName;

        public LavPunctuateProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }


        @Override
        public Processor<Windowed<K>, V> get() {
            return new LavPunctuateProcessor<>(storeName);
        }

    }

    private static class LavPunctuateProcessor<K, V> extends SlidingWindowedPunctuateProcessor<K, V> {
        public LavPunctuateProcessor(String storename) {
            super(null, storename);
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            //  this.context.schedule(this.windows.size, this.windows.advance);
            // --> don't begin at 0, because there may be already elements at pos 0

            this.context.schedule(60, 60);
            kvStore = (KeyValueStore) context.getStateStore(storeName);

        }

        @Override
        public void punctuate(long timestamp) {
            punctated++;


            Triplet<Integer, Integer, Boolean> trip = new Triplet<>(0, 11, true);
            Triplet<Integer, Integer, Boolean> trip2 = new Triplet<>(0, 11, true);
            ComparableSlidingWindowWrapper<Triplet<Integer, Integer, Boolean>> wrapp = new ComparableSlidingWindowWrapper<>(new Windowed<>(trip, new TimeWindow(0, 10)));
            ComparableSlidingWindowWrapper<Triplet<Integer, Integer, Boolean>> wrapp2 = new ComparableSlidingWindowWrapper<>(new Windowed<>(trip2, new TimeWindow(5, 10)));

            logger.debug("equal {}", trip.equals(trip2));
            logger.debug("equal {} {}", wrapp.equals(wrapp2), wrapp.compareTo(wrapp2));

            long matchedWindow;
            long deleteableWindow;

            // special treatment at the beginning
            if (punctated * 60 <= 300) {
                matchedWindow = punctated * 60;
                deleteableWindow = matchedWindow - 60;
            } else if ((punctated * 60 % 300) == 0) {
                matchedWindow = 300 + (punctated - 5) * 300;
                deleteableWindow = matchedWindow - 60;
            } else return;

            // if beginning or five minutes have passes

            logger.debug("LAV Matched window is {}", matchedWindow);
            //   KeyValueIterator<ComparableSlidingWindowWrapper<K>, V> iter = this.kvStore.all();
            KeyValueIterator<ComparableSlidingWindowWrapper<K>, V> iter =
                    //  this.kvStore.range(new ComparableSlidingWindowWrapper(((timestamp-windows.size) > 0 ) ? timestamp-windows.advance: 0), new ComparableSlidingWindowWrapper(timestamp));
                    this.kvStore.range(new ComparableSlidingWindowWrapper<>(matchedWindow - 1), new ComparableSlidingWindowWrapper<>(matchedWindow + 1));
            int i = 0;
            while (iter.hasNext()) {
                i++;
                KeyValue<ComparableSlidingWindowWrapper<K>, V> entry = iter.next();
                if (entry.value.equals(wrapp)) {
                    logger.debug("match");
                }
                context.forward(entry.key.getKey(), new Pair<>(entry.key.getEnd(), entry.value));

            }
            logger.debug("forwarded {}", i);
            iter.close();
            this.cleanOld(deleteableWindow);
            context.commit();

        }

        private void cleanOld(long odlerThen) {
            logger.debug("deleting older than {}", odlerThen);
            KeyValueIterator<ComparableSlidingWindowWrapper<K>, V> iter = this.kvStore.range(new ComparableSlidingWindowWrapper<>(0), new ComparableSlidingWindowWrapper<>(odlerThen + 1));
            while (iter.hasNext()) {
                KeyValue<ComparableSlidingWindowWrapper<K>, V> entry = iter.next();
                context.forward(entry.key.getKey(), new Pair<>(entry.key.getEnd(), entry.value));

            }
            iter.close();
            context.commit();
        }

        @Override
        public void process(Windowed<K> key, V value) {
            kvStore.put(new ComparableSlidingWindowWrapper<>(key), value);
            // special treatment for first element
            if (key.window().end() == 300) {
                for (int i = (int) (300 - 60); i > context.timestamp(); i -= 60) {
                    Window newWindow = new TimeWindow(0, i);
                    kvStore.put(new ComparableSlidingWindowWrapper<>(new Windowed<K>(key.key(), newWindow)), value);

                }
            }
        }


    }


    private static class SlidingWindowedPunctuateProcessor<K, V> implements Processor<Windowed<K>, V> {
        protected ProcessorContext context;
        protected KeyValueStore<ComparableSlidingWindowWrapper<K>, V> kvStore;
        protected String storeName;
        private TimeWindows windows;
        protected long punctated = 0;


        public SlidingWindowedPunctuateProcessor(TimeWindows windows, String storename) {
            this.storeName = storename;
            this.windows = windows;

        }


        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            //  this.context.schedule(this.windows.size, this.windows.advance);
            // --> don't begin at 0, because there may be already elements at pos 0

            this.context.schedule(this.windows.size, this.windows.advance);
            kvStore = (KeyValueStore) context.getStateStore(storeName);

        }

        @Override
        public void process(Windowed<K> key, V value) {
            kvStore.put(new ComparableSlidingWindowWrapper<>(key), value);
        }

        @Override
        public void punctuate(long timestamp) {
            punctated++;
            /**
             * Save for every windowed element a composed key consisting of the key itself
             * and the sliding window-end.
             */

            long matchedWindow = windows.size + punctated * windows.advance;
            logger.debug("Matched window is {}", matchedWindow);
            //   KeyValueIterator<ComparableSlidingWindowWrapper<K>, V> iter = this.kvStore.all();
            KeyValueIterator<ComparableSlidingWindowWrapper<K>, V> iter =
                    //  this.kvStore.range(new ComparableSlidingWindowWrapper(((timestamp-windows.size) > 0 ) ? timestamp-windows.advance: 0), new ComparableSlidingWindowWrapper(timestamp));
                    this.kvStore.range(new ComparableSlidingWindowWrapper<>(matchedWindow - 1), new ComparableSlidingWindowWrapper<>(matchedWindow + 1));

            while (iter.hasNext()) {
                KeyValue<ComparableSlidingWindowWrapper<K>, V> entry = iter.next();
                context.forward(entry.key.getKey(), new Pair<>(entry.key.getEnd(), entry.value));

            }
            iter.close();
            context.commit();


        }

        @Override
        public void close() {
            kvStore.close();
        }
    }
}