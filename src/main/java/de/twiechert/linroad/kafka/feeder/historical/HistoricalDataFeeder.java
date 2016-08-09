package de.twiechert.linroad.kafka.feeder.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * Created by tafyun on 03.08.16.
 */
@Component
public class HistoricalDataFeeder {

    private final String filePath;
    private final TollHistoryRequestHandler tollHistoryRequestHandler;


    private final static Logger logger = (Logger) LoggerFactory
            .getLogger(HistoricalDataFeeder.class);

    @Autowired
    public HistoricalDataFeeder(LinearRoadKafkaBenchmarkApplication.Context context, TollHistoryRequestHandler tollHistoryRequestHandler) {
        this.tollHistoryRequestHandler = tollHistoryRequestHandler;
        this.filePath = context.getHistoricalFilePath();

    }

    public void startFeeding() throws Exception {
        LineIterator iterator = FileUtils.lineIterator(new File(filePath), "UTF-8");
        long linesProcessed = 0;
        try {
            while (iterator.hasNext()) {
                if (linesProcessed % 100000 == 0) {
                    logger.debug("Processed {} 10^5 lines of historical data.", linesProcessed / 100000);

                }
                this.tollHistoryRequestHandler.handle(iterator.nextLine().split(","));
                linesProcessed++;
            }
        } finally {
            LineIterator.closeQuietly(iterator);
        }

    }
}
