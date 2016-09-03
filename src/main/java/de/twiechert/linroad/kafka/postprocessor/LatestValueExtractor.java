package de.twiechert.linroad.kafka.postprocessor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang3.ArrayUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This tool reads a csv file in reverse order and will preserve a record only if it has not been seen.
 * For that purpose a hash of the identifying columns if build and stored in a hashset. For very large files this set may become a
 * bottleneck.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class LatestValueExtractor {

    private final ReversedLinesFileReader fileReader;
    private final int[] identifyingColumns;
    private final int[] skipColumns;
    private final Set<String> scannedEntries = new HashSet<>();
    private final CSVPrinter csvFilePrinter;
    private final Pattern linePattern = Pattern.compile("[(]([\\d., ]+)[)]");


    public LatestValueExtractor(File file, int[] identifyingColumns, int[] skipColumns) throws IOException {
        this.fileReader = new ReversedLinesFileReader(file, Charset.defaultCharset());
        this.identifyingColumns = identifyingColumns;
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file.getPath().split("\\.")[0] + "_reduced.csv"));
        this.csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.RFC4180.withRecordSeparator("\n"));
        this.skipColumns = skipColumns;
    }

    public LatestValueExtractor(File file, int[] identifyingColumns) throws IOException {
        this(file, identifyingColumns, new int[]{});
    }


    public void reduceCsv() throws IOException {
        String line = "";
        List<String> lineList;
        String identifier = "";
        do {
            line = fileReader.readLine();
            if (line != null) {
                line = this.cleanLine(line);

                final String[] lineArr = line.split(", ");
                lineList = IntStream.range(0, lineArr.length)
                        .filter(i -> !ArrayUtils.contains(this.skipColumns, i))
                        .mapToObj(i -> lineArr[i])
                        .collect(Collectors.toList());

                identifier = this.buildIdentifier(lineArr);
                if (!scannedEntries.contains(identifier)) {
                    scannedEntries.add(identifier);
                    csvFilePrinter.printRecord(lineList);
                }
            }
        } while (line != null);
        fileReader.close();
        csvFilePrinter.flush();
        csvFilePrinter.close();
    }

    private String cleanLine(String line) {
        Matcher m = linePattern.matcher(line);
        m.find();
        return m.group(1);
    }

    private String buildIdentifier(String[] records) {
        String identifier = "";
        for (int identifyingColumn : identifyingColumns) {
            identifier += records[identifyingColumn];
        }

        return identifier;

    }
}
