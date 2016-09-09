package de.twiechert.linroad.kafka.model.historical;

import org.javatuples.Pair;

/**
 * This class represents an expenditure at a certain minute.
 *
 *  @author Tayfun Wiechert <tayfun.wiechert@gmail.com
 */
public class ExpenditureAt extends Pair<Long, Double> {

    /**
     * Default constructor may be required depending or serialization library
     */
    public ExpenditureAt() {

    }
    public ExpenditureAt(Long time, Double expenditure) {
        super(time, expenditure);
    }

    public Long getTime() {
        return this.getValue0();
    }

    public Double getExpenditure() {
        return this.getValue1();
    }
}
