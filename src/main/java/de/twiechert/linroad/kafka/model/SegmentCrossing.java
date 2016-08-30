package de.twiechert.linroad.kafka.model;

import org.javatuples.Quartet;
import org.javatuples.Triplet;

/**
 * Created by tafyun on 17.08.16.
 */
public class SegmentCrossing extends Quartet<Long, Integer, Integer, Long> {

    public SegmentCrossing(Long time, Integer segment, Integer lane, Long predecessorTime) {
        super(time, segment, lane, predecessorTime);
    }

    public SegmentCrossing(Long time, Integer segment, Integer lane) {
        super(time, segment, lane, -1L);
    }

    public SegmentCrossing() {
        super(-1L, -1, -1, -1L);
    }

    public SegmentCrossing(SegmentCrossing segmentCrossing, Long predecessorTime) {
        super(segmentCrossing.getTime(), segmentCrossing.getSegment(), segmentCrossing.getLane(), predecessorTime);
    }

    public Long getTime() {
        return this.getValue0();
    }

    public Integer getSegment() {
        return this.getValue1();
    }

    public Integer getLane() {
        return this.getValue2();
    }


    public SegmentCrossing setPredecessorTime(Long predecessorTime) {
        setAt3(predecessorTime);
        return this;
    }

    public Long getPredecessorTime() {
        return getValue3();
    }

}

