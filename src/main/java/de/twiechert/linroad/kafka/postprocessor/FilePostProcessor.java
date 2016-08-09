package de.twiechert.linroad.kafka.postprocessor;

import java.io.File;
import java.io.IOException;

/**
 * Created by tafyun on 09.08.16.
 */
public class FilePostProcessor {

    public static void main(String[] args) throws IOException {
        LatestValueExtractor latestValueExtractor = new LatestValueExtractor(new File("/home/tafyun/IdeaProjects/kafka-linearroad/TOLL_NOT.csv"), new int[]{0, 1, 2}, new int[]{0, 6});
        latestValueExtractor.reduceCsv();
    }
}
