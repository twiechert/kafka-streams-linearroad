package de.twiechert.linroad.kafka.stream.historical;

import de.twiechert.linroad.kafka.LinearRoadKafkaBenchmarkApplication;
import de.twiechert.linroad.kafka.core.Util;
import de.twiechert.linroad.kafka.core.Void;
import de.twiechert.linroad.kafka.core.serde.DefaultSerde;
import de.twiechert.linroad.kafka.model.historical.*;
import de.twiechert.linroad.kafka.stream.StreamBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * This stream represents the response of daily expenditure requests. In order to respond to these requests,
 * the actual request stream is joined with a table of expenditures per vehicle, xway and day.
 *
 * @author Tayfun Wiechert <tayfun.wiechert@gmail.com>
 */
@Component
public class DailyExpenditureResponseStreamBuilder extends StreamBuilder<Void, DailyExpenditureResponse> {

    public final static String TOPIC = "DAILY_EXP_RESP";


    @Autowired
    public DailyExpenditureResponseStreamBuilder(LinearRoadKafkaBenchmarkApplication.Context context, Util util) {
        super(context, util);
    }

    public KStream<Void, DailyExpenditureResponse> getStream(KStream<DailyExpenditureRequest, Void> dailyExpenditureRequestStream,
                                                             KTable<XwayVehicleDay, Double> tollHistory) {

        /**
         * "...such that Type identifies this tuple as an daily expenditure request, Time is the time of the request, VID is the vehicle making the request, QID is the query identifier,
         * and XWay and Day (1 . . .69) identify the expressway and the day (1 is yesterday, 69 is 10 weeks ago) for which an expenditure total is desired. Travel time requests are tuples of the form..."
         */
        KStream<XwayVehicleDay, DailyExpenditureRequest> accountBalanceRequestsPerVehicleXwayAndDay =
                dailyExpenditureRequestStream.map((k, v) -> new KeyValue<>(new XwayVehicleDay(k.getXWay(), k.getVehicleId(), k.getDay()), k))
                        .through(new DefaultSerde<>(), new DefaultSerde<>(), context.topic("ACC_BALANCE_PER_XWAY_VEH_DAY"));


        return accountBalanceRequestsPerVehicleXwayAndDay.leftJoin(tollHistory,
                (dayRequest, currToll) -> new DailyExpenditureResponse(dayRequest.getRequestTime(), this.context.getCurrentRuntimeInSeconds(), dayRequest.getQueryId(), (currToll == null) ? 0 : currToll)
        )
                .selectKey((k, v) -> new Void());
    }

    @Override
    public String getOutputTopic() {
        return TOPIC;
    }
}
