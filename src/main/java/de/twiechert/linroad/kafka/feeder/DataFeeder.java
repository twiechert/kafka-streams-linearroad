package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class DataFeeder {


    private final DataDriverLibrary dataDriverLibrary;

    private final String filePath;

    /**
     * This callback processes tuples generated from the native c implementation.
     */
    public class TupleReceivedCallback implements DataDriverLibrary.TupleReceivedCallback {


        private TupleHandler[] tupleHandlers;

        public TupleReceivedCallback(TupleHandler... tupleHandlers) {
            this.tupleHandlers = tupleHandlers;
            DataFeeder.this.dataDriverLibrary.startProgram(filePath, this);
            Arrays.stream(this.tupleHandlers).forEach(tupleHandler -> tupleHandler.close());
        }

        public void invoke(String s) {
            String[] tuple = s.split(",");
            Arrays.stream(this.tupleHandlers).forEach(tupleHandler -> tupleHandler.handle(tuple));
        }

    }

    @Autowired
    public DataFeeder(DataDriverLibrary dataDriverLibrary, LinearRoadKafkaBenchmarkApplication.Context context) {
        this.dataDriverLibrary = dataDriverLibrary;
        this.filePath = context.getFilePath();
    }

    @Async
    public void startFeeding() {
        new TupleReceivedCallback(new PositionReportHandler(),
                                  new AccountBalanceRequestHandler(),
                                  new DailyExpenditureRequestHandler());

    }

}
