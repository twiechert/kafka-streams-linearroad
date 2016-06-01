package de.twiechert.linroad.kafka.request;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;

/**
 * @author  Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public abstract class Request {

    protected final LinearRoadKafkaBenchmarkApplication.Context context;


    public Request(LinearRoadKafkaBenchmarkApplication.Context context) {
        this.context = context;
    }

    public abstract void execute();
}
