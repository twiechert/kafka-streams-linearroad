package de.twiechert.linroad.kafka.core.serde;
import org.javatuples.*;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * Created by tafyun on 10.07.16.
 */
public class TupleSerdes {


    // Producer< Pair<Integer, Integer> ,  Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>>


    public static class PairSerdes<A, B> extends ByteArraySerde<Pair<A,B>> {


    }

    public static class TripletSerdes<A,B,C> extends ByteArraySerde<Triplet<A,B,C>> {


    }

    public static class QuartetSerdes<A, B, C, D>  extends ByteArraySerde<Quartet<A, B, C, D>> {


    }


    public static class QuintetSerdes<A, B, C, D, E>   extends ByteArraySerde<Quintet<A, B, C, D, E> > {


    }


    public static class SextetSerdes<A, B, C, D, E, F>  extends ByteArraySerde<Sextet<A, B, C, D, E, F>> {


    }

    public static class SeptetSerdes  extends ByteArraySerde<Septet> {


    }


}
