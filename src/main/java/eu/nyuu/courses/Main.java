package eu.nyuu.courses;

import eu.nyuu.courses.model.*;
import eu.nyuu.courses.serdes.SerdeFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class Main {

    public static void main(final String[] args) {

        final String bootstrapServers = args.length > 0 ? args[0] : "51.15.90.153:9092";
        final Properties streamsConfiguration = new Properties();
        Tools tools = new Tools();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app-sly");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "my-stream-app-client");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        tools.deleteFolder("C:\\Users\\perso\\IdeaProjects\\twitter-sentiment-analysis-5IBD\\tmp");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\perso\\IdeaProjects\\twitter-sentiment-analysis-5IBD\\tmp");
        streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);

        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        final Map<String, Object> serdeProps = new HashMap<>();
        final Serde<Tweet> tweetSerde = SerdeFactory.createSerde(Tweet.class, serdeProps);
        final Serde<TweetWithSentiment> tweetWithSentimentSerde = SerdeFactory.createSerde(TweetWithSentiment.class, serdeProps);
        final Serde<Metric> metricSerde = SerdeFactory.createSerde(Metric.class, serdeProps);
        final Serde<TweetWithMetric> tweetWithMetricSerde = SerdeFactory.createSerde(TweetWithMetric.class, serdeProps);

        // Stream
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Tweet> tweets = builder.stream("tweets", Consumed.with(stringSerde, tweetSerde));
''
        SentimentAnalysis sa = new SentimentAnalysis();

        KGroupedStream<String, TweetWithSentiment> tweetByName = tweets
                .map((key, value) -> KeyValue.pair(value.getNick(), new TweetWithSentiment(value, sa.getSentiment(value.getBody()))))
                .groupByKey(Grouped.with(stringSerde, tweetWithSentimentSerde));

        KGroupedStream<String, TweetWithSentiment> tweetByDate = tweets
                .map((key, value) -> KeyValue.pair("KEY", new TweetWithSentiment(value, sa.getSentiment(value.getBody()))))
                .groupByKey(Grouped.with(stringSerde, tweetWithSentimentSerde));

        KGroupedStream<String, TweetWithSentiment> TweetByNameWithHashtag = tweets
                .map((key, value) -> KeyValue.pair(value.getNick(), new TweetWithSentiment(value, sa.getSentiment(value.getBody()))))
                .filter((key, value) -> value.getTweet().getBody().contains("#"))
                .groupByKey(Grouped.with(stringSerde, tweetWithSentimentSerde));


        TimeWindowedKStream<String, TweetWithSentiment> TimewindowedTweetKStream = tweetByDate
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)));

        TimeWindowedKStream<String, TweetWithSentiment> TimewindowedDayTweetKStream = tweetByDate
                .windowedBy(TimeWindows.of(Duration.ofDays(1)));

        TimeWindowedKStream<String, TweetWithSentiment> TimewindowedMonthTweetKStream = tweetByDate
                .windowedBy(TimeWindows.of(Duration.ofDays(30)));

        TimeWindowedKStream<String, TweetWithSentiment> TimewindowedYearTweetKStream = tweetByDate
                .windowedBy(TimeWindows.of(Duration.ofDays(365)));


        KTable<Windowed<String>, TweetWithMetric> windowedLastUserKTable = TimewindowedTweetKStream
                .aggregate(
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
                                .<String, TweetWithMetric, WindowStore<Bytes, byte[]>>as("tweetStoreUsers")
                                .withKeySerde(stringSerde).withValueSerde(tweetWithMetricSerde)
                );
        windowedLastUserKTable.toStream().print(Printed.toSysOut());

        windowedLastUserKTable.toStream().to("user-tweet-store-metrics");

        KTable<Windowed<String>, TweetWithMetric> windowedLastMetricKTable = TimewindowedTweetKStream
                .aggregate(
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

        windowedLastMetricKTable.toStream().to("last-tweet-store-metrics");
        // windowedLastMetricKTable.toStream().print(Printed.toSysOut());


        KTable<Windowed<String>, TweetWithMetric> windowedDayMetricKTable = TimewindowedDayTweetKStream
                .aggregate(
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
                                .<String, TweetWithMetric, WindowStore<Bytes, byte[]>>as("tweetDayStoreMetrics")
                                .withKeySerde(stringSerde).withValueSerde(tweetWithMetricSerde)
                );

        windowedDayMetricKTable.toStream().to("tweet-day-store-metrics");


        KTable<Windowed<String>, TweetWithMetric> windowedMetricMonthKTable = TimewindowedMonthTweetKStream
                // .peek((msg, value)->System.out.println(value))
                .aggregate(
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
                                .<String, TweetWithMetric, WindowStore<Bytes, byte[]>>as("tweetMonthStoreMetrics")
                                .withKeySerde(stringSerde).withValueSerde(tweetWithMetricSerde)
                );

        windowedMetricMonthKTable.toStream().to("tweet-month-store-metrics");

        KTable<Windowed<String>, TweetWithMetric> windowedMetricYearKTable = TimewindowedYearTweetKStream
                // .peek((msg, value)->System.out.println(value))
                .aggregate(
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
                                .<String, TweetWithMetric, WindowStore<Bytes, byte[]>>as("tweetYearStoreMetrics")
                                .withKeySerde(stringSerde).withValueSerde(tweetWithMetricSerde)
                );

        windowedMetricYearKTable.toStream().to("tweet-year-store-metrics");


        TweetByNameWithHashtag
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .count()
                .filter((key, value) -> value > 1000)
                .toStream()
                .to("Last_tweet-popular-hashtag");


        TweetByNameWithHashtag
                .windowedBy(TimeWindows.of(Duration.ofDays(30)))
                .count()
                .filter((key, value) -> value > 1000)
                .toStream()
                .to("Last30_tweet-popular-hashtag");


        // Window by 30s, 1mn & 5mn
      /*  repartitionedVisits
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("url-count-30s")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long())
                );
        SentimentAnalysis sa = new SentimentAnalysis();
*/
        // aggregation par user
       /* KStream<String, TweetWithSentiment> user_tweet = tweetStream
                .map((key, value) -> KeyValue.pair(value.getNick(), new TweetWithSentiment(value, sa.getSentiment(value.getBody()))));

        KTable<Windowed<String>, MetricEvent> user = result_user.groupByKey(Grouped.with(Serdes.String(), SerdeFactory.createSerde(TwitterEventWithSentiment.class, serdeProps)))
                .windowedBy(TimeWindows.of(Duration.ofMillis(10000)))
                .aggregate(
                        () -> new MetricEvent(),
                        (aggKey, newValue, aggValue) -> new MetricEvent(aggValue, newValue),
                        Materialized
                                .<String, MetricEvent, WindowStore< Bytes, byte[]>> as ("twitterStoreUser").withValueSerde(MetricEventSerde)
                );*/
//        user.toStream().print(Printed.toSysOut());
                /*      .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
              */

    /*    tweetStream.map((key, value) -> KeyValue.pair(value.getNick(), value))
                .peek((msg, value)->System.out.println(value))
                .groupByKey()
                .windowedBy((TimeWindows.of(Duration.ofSeconds(30))))
                .count()
                .toStream().print(Printed.toSysOut());*/

      /*  tweetStream.foreach((key, value) -> {
          //  System.out.println(key+" : "+value);
            System.out.println(sa.getSentiment(value.getBody()));
        });
*/


//        final Topology topology = builder.build();



       /* tweetStream
            .map((key, value) -> KeyValue.pair(value.getUrl(), value))
            .peek((msg, value)->log.debug("", msg, value))
            .groupByKey()
            .windowedBy((TimeWindows.of(Duration.ofSeconds(30))))
            .count();
*/


        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(1);
        }
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

};