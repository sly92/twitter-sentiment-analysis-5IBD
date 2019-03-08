package eu.nyuu.courses.model;
import edu.stanford.nlp.simple.*;

public class SentimentAnalysis {

    public String getSentiment(String sentence) {

        Sentence sent = new Sentence(sentence.toLowerCase().toString());
        SentimentClass sentiment = sent.sentiment();
        return sentiment.toString();
    }
}
