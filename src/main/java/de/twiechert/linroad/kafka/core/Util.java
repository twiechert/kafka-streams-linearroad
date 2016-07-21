package de.twiechert.linroad.kafka.core;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by tafyun on 01.06.16.
 */
@Component
public class Util {

    private LinearRoadKafkaBenchmarkApplication.Context context;

    @Autowired
    public Util(LinearRoadKafkaBenchmarkApplication.Context context) {
       this.context = context;
    }

    public static long minuteOfReport(long timestamp) {
        return (timestamp/60)+1;
    }

    public static String str(Object ob) {
        return ob.toString();
    }

    public static Integer pInt(String ob) {
        return Integer.parseInt(ob);
    }


    public static Long pLng(String ob) {
        return Long.parseLong(ob);
    }

    public static Double pDob(String ob) {
        return Double.parseDouble(ob);
    }


}
