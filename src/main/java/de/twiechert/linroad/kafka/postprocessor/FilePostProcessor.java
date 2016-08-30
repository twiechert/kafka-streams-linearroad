package de.twiechert.linroad.kafka.postprocessor;

import java.io.File;
import java.io.IOException;

/**
 * Created by tafyun on 09.08.16.
 */
public class FilePostProcessor {

    public static void main(String[] args) throws IOException {
        LatestValueExtractor latestTollExtractor = new LatestValueExtractor(new File("TOLL_NOT.csv"), new int[]{0, 1, 2}, new int[]{6});
        latestTollExtractor.reduceCsv();

        LatestValueExtractor accidentExtractor = new LatestValueExtractor(new File("ACC_NOT.csv"), new int[]{0, 1, 2});
        accidentExtractor.reduceCsv();

        LatestValueExtractor accountBalanceExtractor = new LatestValueExtractor(new File("ACCOUNT_BALANCE_RESP.csv"), new int[]{0, 1, 2});
        accountBalanceExtractor.reduceCsv();

    }
}
