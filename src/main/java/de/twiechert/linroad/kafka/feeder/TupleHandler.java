package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
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

    private int handleOn = -1;
    private Producer<Outputkey, OutputValue> producer;
    private Properties producerConfig = new Properties();
    private LinearRoadKafkaBenchmarkApplication.Context context;


    public TupleHandler(LinearRoadKafkaBenchmarkApplication.Context context) {
        this.context = context;
        this.producer = new KafkaProducer<>(this.mergeProperties());

    }

    public TupleHandler(LinearRoadKafkaBenchmarkApplication.Context context, int handleOn) {
        this.handleOn = handleOn;
        this.context = context;
        this.producer = new KafkaProducer<>(this.mergeProperties());

    }

    public boolean handle(String[] tuple) {
        if (handleOn == -1 || pInt(tuple[0]) == handleOn) {
            producer.send(new ProducerRecord<>(context.topic(this.getTopic()), this.transformKey(tuple), this.transformValue(tuple)));
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
        this.producerConfig.putAll(context.getProducerBaseConfig());
        this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.getKeySerializerClass().getName());
        this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.getValueSerializerClass().getName());
        return this.producerConfig;
    }

    public void close() {
        this.producer.close();
    }
}

