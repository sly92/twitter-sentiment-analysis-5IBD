package eu.nyuu.courses.model;

public class TweetWithSentiment {

        private Tweet tweet;
        private String sentiment;

        public TweetWithSentiment() {}
        
        public TweetWithSentiment(Tweet value, String sentiment) {
            this.tweet = value;
            this.sentiment=sentiment;
        }

        public Tweet getTweet() {
            return tweet;
        }

        public void setTweet(Tweet tweet) {
            this.tweet = tweet;
        }

        public String getSentiment() {
            return sentiment;
        }

        public void setSentiment(String sentiment) {
            this.sentiment = sentiment;
        }

        @Override
        public String toString() {
            return tweet + "sentiment='" + sentiment+"\n";
        }
}
