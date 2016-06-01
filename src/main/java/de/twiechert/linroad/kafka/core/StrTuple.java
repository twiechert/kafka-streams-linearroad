package de.twiechert.linroad.kafka.core;

/**
 * Created by tafyun on 01.06.16.
 */
public class StrTuple {

    private String[] array;

    public StrTuple(String... array) {
        this.array = array;
    }

    public StrTuple(String[] array1, String... array2) {
        array = new String[array1.length + array2.length];
        System.arraycopy(array1, 0, array, 0, array1.length);
        System.arraycopy(array2, 0, array, array1.length, array2.length);
    }

    public String[] getArray() {
        return array;
    }

    public void setArray(String[] array) {
        this.array = array;
    }

    @Override
    public String toString() {
        return String.join(",", array);

    }

    public String get(int in) {
        return this.array[in];
    }

    public String get(Enum en) {
        return this.array[en.ordinal()];
    }


    public String[] get(Enum... en) {
        String [] arr = new String[en.length];
        int i = 0;
        for(Enum e:en) {
            arr[i++] = this.get(e);
        }
        return arr;
    }

}
