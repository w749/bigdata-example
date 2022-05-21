package org.example.exercise.topn;

import java.sql.Timestamp;

public class UrlViewCount {
    public String url;
    public long pv;
    public long start;
    public long end;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, long pv, long start, long end) {
        this.url = url;
        this.pv = pv;
        this.start = start;
        this.end = end;
    }

    public String getUrl() {
        return url;
    }

    public long getPv() {
        return pv;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", pv=" + pv +
                ", start=" + new Timestamp(start) +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
