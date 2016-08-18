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
    private MinuteTimer minuteTimer;

    @Autowired
    private LinearRoadKafkaBenchmarkApplication.Context context;


    /**
     * This callback processes tuples generated from the native c implementation.
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

        public void invoke(String s) {
            if (!firstArived) {
                this.context.markAsStarted();
                //this.startMinuteFeeder();
                firstArived = true;
            }
            String[] tuple = s.split(",");
            Arrays.stream(this.tupleHandlers).forEach(tupleHandler -> tupleHandler.handle(tuple));
        }

        @Async
        private void startMinuteFeeder() {
            new Timer().scheduleAtFixedRate(new TimerTask() {
                public void run() {
                    minuteTimer.handle(null);
                }
            }, 0, 60 * 1000);

        }

    }

    @Autowired
    public DataFeeder(DataDriverLibrary dataDriverLibrary, LinearRoadKafkaBenchmarkApplication.Context context) {
        this.dataDriverLibrary = dataDriverLibrary;
        this.filePath = context.getFilePath();
    }

    @Async
    public void startFeedingAsync() {
        new TupleReceivedCallback(context, positionReportHandler,
                dailyExpenditureRequestHandler,
                accountBalanceRequestHandler);

    }

    public void startFeeding() {
        new TupleReceivedCallback(context, positionReportHandler,
                dailyExpenditureRequestHandler,
                accountBalanceRequestHandler);

    }

}
