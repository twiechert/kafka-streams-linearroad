package de.twiechert.linroad.kafka.core.serde.provider;

import de.twiechert.linroad.kafka.model.*;
import de.twiechert.linroad.kafka.model.historical.*;
import de.twiechert.linroad.kafka.stream.LatestAverageVelocityStreamBuilder;
import de.twiechert.linroad.kafka.stream.NumberOfVehiclesStreamBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by tafyun on 07.08.16.
 */
public class FSTSerde<T extends Serializable> implements Serde<T> {


    private static final Logger logger = LoggerFactory
            .getLogger(FSTSerde.class);

    static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();


    /**
     * PERFORMANCE BOOST --> Serialiezer does not need to write whole class names.
     */
    static {
        conf.registerClass(AccountBalanceRequest.class,
                AccountBalanceResponse.class,
                DailyExpenditureRequest.class,
                DailyExpenditureResponse.class,
                XwayVehicleDay.class,
                AccidentNotification.class,
                AverageVelocity.class,
                CurrentToll.class,
                NumberOfVehicles.class,
                PositionReport.class,
                TollNotification.class,
                VehicleIdXwayDirection.class,
                XwaySegmentDirection.class,
                NumberOfVehiclesStreamBuilder.VehicleIdTimeIntermediate.class,
                LatestAverageVelocityStreamBuilder.LatestAverageVelocityIntermediate.class
        );

    }


    public static class FSTSerializer<A> implements Serializer<A> {
        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String s, A a) {
            return conf.asByteArray(a);
        }

        @Override
        public void close() {

        }
    }

    public static class FSTDeserializer<A> implements Deserializer<A> {


        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public A deserialize(String s, byte[] bytes) {


            return (bytes != null) ? (A) conf.asObject(bytes) : null;
        }

        @Override
        public void close() {

        }
    }


    @Override
    public Serializer<T> serializer() {
        return new FSTSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new FSTDeserializer<>();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }


}