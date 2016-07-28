package de.twiechert.linroad.kafka.feeder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

import static de.twiechert.linroad.kafka.core.Util.pInt;

/**
 * Created by tafyun on 21.07.16.
 */
public abstract class TupleHandler<Outputkey, OutputValue> {

    private final int handleOn;
    private Producer<Outputkey, OutputValue> producer;
    private Properties producerConfig = new Properties();

    {
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.2:9092, 172.17.0.3:9092, 172.17.0.4:9092");
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

    }

    public TupleHandler(int handleOn) {
        this.handleOn = handleOn;
        this.producer = new KafkaProducer<>(this.mergeProperties());

    }

    public boolean handle(String[] tuple) {
        if(pInt(tuple[0]) == handleOn) {
            producer.send(new ProducerRecord<Outputkey, OutputValue>(this.getTopic(), this.transformKey(tuple), this.transformValue(tuple)));
            return true;
        }
        return false;
    }

    protected abstract Outputkey transformKey(String[] tuple);

    protected abstract OutputValue transformValue(String[] tuple);

    protected abstract Class<? extends Serializer<Outputkey>> getKeySerializerClass();

    protected abstract Class<? extends Serializer<OutputValue>> getValueSerializerClass();

    protected abstract String getTopic();

    public int getHandleOn() {
        return handleOn;
    }

    private Properties mergeProperties() {
        this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.getKeySerializerClass().getName());
        this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.getValueSerializerClass().getName());
        return this.producerConfig;
    }

    public void close() {
        this.producer.close();
    }
}

