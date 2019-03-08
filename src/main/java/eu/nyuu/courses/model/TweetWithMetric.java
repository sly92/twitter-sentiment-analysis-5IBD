package eu.nyuu.courses.model;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class TweetWithMetric {

    private String timestamp;
    private int count_positive;
    private int count_negative;
    private int count_neutral;
    private List<String> hashtags;

    public TweetWithMetric() {
        timestamp = new Date().toString();
        count_negative = 0;
        count_positive = 0;
        count_neutral = 0;
        hashtags = null;
    }

    public TweetWithMetric(String timestamp, int count_positive, int count_negative, int count_neutral, List<String> hashtags) {
        this.timestamp = timestamp;
        this.count_positive = count_positive;
        this.count_negative = count_negative;
        this.count_neutral = count_neutral;
        this.hashtags = hashtags;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getCount_positive() {
        return count_positive;
    }

    public void setCount_positive(int count_positive) {
        this.count_positive = count_positive;
    }

    public int getCount_negative() {
        return count_negative;
    }

    public void setCount_negative(int count_negative) {
        this.count_negative = count_negative;
    }

    public int getCount_neutral() {
        return count_neutral;
    }

    public void setCount_neutral(int count_neutral) {
        this.count_neutral = count_neutral;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }


    public void updateCount(String sentiment, String body) {

        System.out.println(body);
        if (body.contains("#")) {
            Pattern p = Pattern.compile("#*");
            if (this.hashtags.contains(p))
                this.hashtags.add(p.toString());
        }

        switch (sentiment) {
            case "NEGATIVE":
                this.setCount_negative(this.getCount_negative() + 1);
                break;
            case "NEUTRAL":
                this.setCount_positive(this.getCount_neutral() + 1);
                break;
            case "POSITIVE":
                this.setCount_positive(this.getCount_positive() + 1);
                break;
            case "VERY_NEGATIVE":
                this.setCount_negative(this.getCount_negative() + 1);
                break;
            case "VERY_POSITIVE":
                this.setCount_positive(this.getCount_positive() + 1);
                break;
            default:
                System.out.println("Sentiment doesn't exist");
                break;
        }


    }

    public void append(TweetWithMetric twm){
        this.count_negative = this.count_negative + twm.getCount_negative();
        this.count_positive = this.count_positive + twm.getCount_positive();
        this.count_neutral = this.count_neutral + twm.getCount_neutral();
    }


    /*public KTable<Windowed<String>, TweetWithMetric> createTable(TimeWindowedKStream twk, Serde<String> stringSerde, Serde<TweetWithMetric> tweetWithMetricSerde) {

        return twk.aggregate(
                TweetWithMetric::new,
                (aggKey, newValue, aggValue) -> {
                    TweetWithMetric twm = new TweetWithMetric(
                            aggValue.getTimestamp(),
                            aggValue.getCount_positive(),
                            aggValue.getCount_negative(),
                            aggValue.getCount_neutral(),
                            aggValue.getHashtags()
                    );
                    aggValue.updateCount(newValue.getSentiment(), newValue.getTweet().getBody());
                    return twm;
                },
                Materialized
                        .<String, TweetWithMetric, WindowStore<Bytes, byte[]>>as("tweetStoreMetrics")
                        .withKeySerde(stringSerde).withValueSerde(tweetWithMetricSerde)
        );

    }*/
}
