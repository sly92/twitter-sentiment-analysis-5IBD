package eu.nyuu.courses.model;

import java.util.Date;

public class Metric {

        private String id;
        private String timestamp;
        private int count;

    public Metric() {}

    public Metric(String id, String timestamp, int count) {
        this.id = id;
        this.timestamp = timestamp;
        this.count = count;
    }


    public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
}
