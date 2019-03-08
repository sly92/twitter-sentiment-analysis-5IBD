package eu.nyuu.courses.controller;

import eu.nyuu.courses.model.TweetQuery;
import eu.nyuu.courses.model.TweetWithMetric;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.WindowStoreIterator;
import javax.ws.rs.BadRequestException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class MetricsController {

  /*  @RequestMapping("/store/user")
    public TweetWithMetric getLastMetrics(@RequestParam(value="metrics", defaultValue="metrics") String name) {
        TweetQuery tq = new TweetQuery();
        return tq.sentimentsByUser("user-tweet-store-metrics");
    }

    @RequestMapping("/store/last")
    public TweetWithMetric getLastMetrics(@RequestParam(value="metrics", defaultValue="metrics") String name) {
        TweetQuery tq = new TweetQuery();
        return tq.sentimentByDate(,)
    }


    @RequestMapping("/store/lastMonth")
    public TweetWithMetric getLastMetrics(@RequestParam(value="metrics", defaultValue="metrics") String name) {
        return tq.sentimentByDate()
    }

    @RequestMapping("/store/lastYear")
    public TweetWithMetric getLastMetrics(@RequestParam(value="metrics", defaultValue="metrics") String name) {
        return tq.sentimentByDate()
    }

    @RequestMapping("/store/topHashtagDay")
    public TweetWithMetric getLastMetrics(@RequestParam(value="metrics", defaultValue="metrics") String name) {
        return tq.sentimentByDate()
    }

    @RequestMapping("/store/topHashtagMonth")
    public TweetWithMetric getLastMetrics(@RequestParam(value="metrics", defaultValue="metrics") String name) {
        return tq.sentimentByDate()
    }*/


}
