package eu.nyuu.courses.model;

import eu.nyuu.courses.model.TweetWithMetric;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Class who wrap Kafka interactive queries to handle sentiments by tweet and types
 */
public class TweetQuery {
    private String key;
    private TweetWithMetric tweetSentiment;

    public TweetQuery(String key, TweetWithMetric aggTweet) {
        this.key = key;
        this.tweetSentiment = aggTweet;
    }

    public TweetQuery() {}

    public String getKey() { return key; }

    public void setKey(String key) { this.key = key; }

    public TweetWithMetric getTweetSentiment() { return tweetSentiment; }

    public void setTweetSentiment(TweetWithMetric aggTweet) { this.tweetSentiment = aggTweet; }

    public List<TweetQuery> sentimentsByUser(KafkaStreams streams) throws Exception {
        // if (streams.state() != KafkaStreams.State.RUNNING) throw new Exception("KafkaStreams not running");
        List<TweetQuery> sentimentsByUser = new ArrayList<>();
        ReadOnlyKeyValueStore<String,TweetWithMetric> aggregateTweetStore =
                streams.store("user-tweet-store-metrics", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, TweetWithMetric> userSentimentIterator = aggregateTweetStore.all();
        while (userSentimentIterator.hasNext()) {
            KeyValue<String, TweetWithMetric> next = userSentimentIterator.next();
            sentimentsByUser.add(new TweetQuery(next.key, next.value));
        }
        userSentimentIterator.close();
        return sentimentsByUser;
    }

    public List<TweetQuery> sentimentByDate(KafkaStreams streams, String storeName, Long duration) {
        List<TweetQuery> sentimentsByDate = new ArrayList<>();
        ReadOnlyKeyValueStore<String,TweetWithMetric> aggregateTweetStore =
                streams.store("user-tweet-store-metrics", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, TweetWithMetric> userSentimentIterator = aggregateTweetStore.all();
        while (userSentimentIterator.hasNext()) {
            KeyValue<String, TweetWithMetric> next = userSentimentIterator.next();
            sentimentsByDate.add(new TweetQuery(next.key, next.value));
        }
        userSentimentIterator.close();
        return sentimentsByDate;
    }
}
