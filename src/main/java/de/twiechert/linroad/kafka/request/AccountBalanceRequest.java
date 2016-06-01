package de.twiechert.linroad.kafka.request;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;

/**
 * @author  Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class AccountBalanceRequest extends Request{

    private final String vehicleIdentifier;

    private final String queryIdentifier;

    public AccountBalanceRequest(LinearRoadKafkaBenchmarkApplication.Context context, String vehicleIdentifier, String queryIdentifier) {
        super(context);
        this.vehicleIdentifier = vehicleIdentifier;
        this.queryIdentifier = queryIdentifier;
    }

    @Override
    public void execute() {

    }
}
