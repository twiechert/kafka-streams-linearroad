package de.twiechert.linroad.kafka.core.serde;

/**
 * Created by tafyun on 04.08.16.
 */

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.util.ObjectBuffer;
import de.twiechert.linroad.kafka.model.*;
import de.twiechert.linroad.kafka.model.historical.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;


/**
 * Created by tafyun on 10.07.16.
 */
public class KryoSerde<T extends Serializable> implements Serde<T> {


    public static void registerClasses(Kryo kryo) {
        kryo.register(AccountBalanceRequest.class, 0);
        kryo.register(AccountBalanceResponse.class, 1);
        kryo.register(DailyExpenditureRequest.class, 2);
        kryo.register(DailyExpenditureResponse.class, 3);
        kryo.register(XwayVehicleDay.class, 4);
        kryo.register(AccidentNotification.class, 5);
        kryo.register(AverageVelocity.class, 6);
        kryo.register(CurrentToll.class, 7);
        kryo.register(NumberOfVehicles.class, 8);
        kryo.register(PositionReport.class, 10);
        kryo.register(TollNotification.class, 11);
        kryo.register(VehicleIdXwayDirection.class, 12);
        kryo.register(XwaySegmentDirection.class, 13);

    }

    public static class KryoSerializer<A> implements Serializer<A> {


        @SuppressWarnings("unchecked")
        public KryoSerializer() {

        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String s, A a) {
            Kryo kryo = new Kryo();
            KryoSerde.registerClasses(kryo);
            Output out = new Output(new ByteArrayOutputStream());
            kryo.writeClassAndObject(out, a);
            byte[] res = out.getBuffer();
            out.close();
            return res;
        }

        @Override
        public void close() {

        }
    }

    public static class KryoDeserializer<A> implements Deserializer<A> {

        @SuppressWarnings("unchecked")
        public KryoDeserializer() {

        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public A deserialize(String s, byte[] bytes) {
            Kryo kryo = new Kryo();
            KryoSerde.registerClasses(kryo);
            Input input = new Input(new ByteArrayInputStream(bytes));
            return (A) kryo.readClassAndObject(input);

        }

        @Override
        public void close() {

        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new KryoSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
        return new KryoDeserializer<>();
    }
}
