package de.twiechert.linroad.kafka.core.serde;
import org.javatuples.*;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by tafyun on 10.07.16.
 */
public class TupleSerdes {


    // Producer< Pair<Integer, Integer> ,  Sextet<Integer, Integer, Integer, Boolean, Integer, Integer>>


    public static class PairSerdes<A, B> extends DefaultSerde<Pair<A, B>> {

        public PairSerdes(Class<Pair<A, B>> classOb) {
            super(classOb);
        }
    }

    public static class TripletSerdes<A, B, C> extends DefaultSerde<Triplet<A, B, C>> {
        public TripletSerdes(Class<Triplet<A, B, C>> classOb) {
            super(classOb);
        }
    }

    public static class QuartetSerdes<A, B, C, D> extends DefaultSerde<Quartet<A, B, C, D>> {

        public QuartetSerdes(Class<Quartet<A, B, C, D>> classOb) {
            super(classOb);
        }
    }


    public static class QuintetSerdes<A, B, C, D, E> extends DefaultSerde<Quintet<A, B, C, D, E>> {
        public QuintetSerdes(Class<Quintet<A, B, C, D, E>> classOb) {
            super(classOb);
        }
    }


    public static class SextetSerdes<A, B, C, D, E, F> extends DefaultSerde<Sextet<A, B, C, D, E, F>> {

        public SextetSerdes(Class<Sextet<A, B, C, D, E, F>> classOb) {
            super(classOb);
        }
    }

    public static class SeptetSerdes<A, B, C, D, E, F, G> extends DefaultSerde<Septet<A, B, C, D, E, F, G>> {
        public SeptetSerdes(Class<Septet<A, B, C, D, E, F, G>> classOb) {
            super(classOb);
        }
    }






}
