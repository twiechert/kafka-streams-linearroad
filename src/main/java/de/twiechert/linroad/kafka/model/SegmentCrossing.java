package de.twiechert.linroad.kafka.model;

import org.javatuples.Quartet;

/**
 * This class represents a segment crossing, i.e. a the first position report per segment and vehicle.
 * It is required, because vehicles emit multiple such reports per segment and for toll notification, we must only consider the first.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
public class SegmentCrossing extends Quartet<Long, Integer, Integer, Long> {


    public SegmentCrossing(Long time, Integer segment, Integer lane, Long predecessorTime) {
        super(time, segment, lane, predecessorTime);
    }

    public SegmentCrossing(Long time, Integer segment, Integer lane) {
        super(time, segment, lane, -1L);
    }

    /**
     * Default constructor may be required depending or serialization library
     */
    public SegmentCrossing() {
        super(-1L, -1, -1, -1L);
    }

    private SegmentCrossing(SegmentCrossing segmentCrossing, Long predecessorTime) {
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
        return new SegmentCrossing(this, predecessorTime);
    }

    public Long getPredecessorTime() {
        return getValue3();
    }

}

