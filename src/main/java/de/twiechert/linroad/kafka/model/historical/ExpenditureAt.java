package de.twiechert.linroad.kafka.model.historical;

import org.javatuples.Pair;

/**
 * Created by tafyun on 07.08.16.
 */
public class ExpenditureAt extends Pair<Long, Double> {

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
