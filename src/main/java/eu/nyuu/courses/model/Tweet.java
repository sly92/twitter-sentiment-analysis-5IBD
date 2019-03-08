package eu.nyuu.courses.model;

public class Tweet {
    private String id;
    private String timestamp;
    private String nick;
    private String body;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getNick() {
        return nick;
    }

    public void setNick(String nick) {
        this.nick = nick;
    }

    public String getBody() {
        return body.replaceAll("[^\\x00-\\x7F]", "");
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return String.format("Tweet : \nid = %s \ntime = %s \nnick = %s \nbody = %s \n", this.id, this.timestamp, this.nick, this.body);
    }
}
