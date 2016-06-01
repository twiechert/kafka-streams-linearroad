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

    public String minuteOfReport(String timestamp) {
        int timestampInt = Integer.parseInt(timestamp);
        return (timestampInt/60)+1+"";
    }
}
