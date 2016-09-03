package de.twiechert.linroad.kafka.stream;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.feeder.PositionReportHandler;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;

import java.io.Serializable;
import java.util.Properties;

/**
 * Classes that create streams may extend this class.
 * This is generally helpful if the respective stream is materialized.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public abstract class StreamBuilder<OutputKey extends Serializable, OutputValue extends Serializable> {

    private final Serde<OutputKey> keySerde;

    private final Serde<OutputValue> valueSerde;


    protected final LinearRoadKafkaBenchmarkApplication.Context context;

    public StreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context,
                         Serde<OutputKey> keySerde,
                         Serde<OutputValue> valueSerde) {
        this.context = context;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public StreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context) {
        this(context, new DefaultSerde<>(), new DefaultSerde<>());
    }

    /**
     * @return the Serde used for the key of the generated stream.
     */
    public Serde<OutputKey> getKeySerde() {
        return keySerde;
    }

    /**
     * @return the Serde used for the value of the generated stream.
     */
    public Serde<OutputValue> getValueSerde() {
        return valueSerde;
    }

    /**
     *
     * @return the name of the topic the stream is materialized to.
     */
    public abstract String getOutputTopic();

}
