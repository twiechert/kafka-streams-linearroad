package de.twiechert.linroad.kafka.feeder;

import de.twiechert.linroad.jdriver.DataDriverLibrary;
import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This class feeds the respective kafka topics with LR tuples.
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class DataFeeder {


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(DataFeeder.class);

    private final DataDriverLibrary dataDriverLibrary;

    private final String filePath;

    @Autowired
    private PositionReportHandler positionReportHandler;

    @Autowired
    private DailyExpenditureRequestHandler dailyExpenditureRequestHandler;

    @Autowired
    private AccountBalanceRequestHandler accountBalanceRequestHandler;

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;


    /**
     * This callback processes tuples generated from the native c implementation.
     * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
     */
    public class TupleReceivedCallback implements DataDriverLibrary.TupleReceivedCallback {


        private TupleHandler[] tupleHandlers;
        private LinearRoadKafkaBenchmarkApplication.Context context;
        private boolean firstArived = false;

        public TupleReceivedCallback(LinearRoadKafkaBenchmarkApplication.Context context, TupleHandler... tupleHandlers) {
            this.tupleHandlers = tupleHandlers;
            this.context = context;
            DataFeeder.this.dataDriverLibrary.startProgram(filePath, this);
            Arrays.stream(this.tupleHandlers).forEach(TupleHandler::close);
        }

        /**
         * This method is called for every line emitted by the data driver.
         * @param line the line representing a LR tuple
         */
        public void invoke(String line) {
            if (!firstArived) {
                logger.debug("First element has arrived, starting timer.");
                LinearRoadKafkaBenchmarkApplication.Context.markAsStarted();
                firstArived = true;
            }
            String[] tuple = line.split(",");
            // find a tuple handler that is able to process that tuple
            Arrays.stream(this.tupleHandlers).forEach(tupleHandler -> tupleHandler.handle(tuple));
        }
    }

    @Autowired
    public DataFeeder(DataDriverLibrary dataDriverLibrary, LinearRoadKafkaBenchmarkApplication.Context context) {
        this.dataDriverLibrary = dataDriverLibrary;
        this.filePath = context.getFilePath();
    }

    /**
     * Will start feeding the Kafka topics with the respective LR tuples.
     * This method is synchronous. You may add @Async to allow asynchronous execution.
     */
    public void startFeeding() {
        new TupleReceivedCallback(context, positionReportHandler,
                dailyExpenditureRequestHandler,
                accountBalanceRequestHandler);
    }

}
