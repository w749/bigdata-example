package org.example.util;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * Flink可操作对象Event
 * 注意必须要确保所有属性都是public共有的，并且是可序列化的，最后必须提供空参构造器
 */
public class Event {
    private String user;
    private String url;
    private Long time;

    public Event() {
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getUser() {
        return user;
    }

    public String getUrl() {
        return url;
    }

    public Long getTime() {
        return time;
    }

    public Event(String user, String url, Long time) {
        this.user = user;
        this.url = url;
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(user, event.user) && Objects.equals(url, event.url) && Objects.equals(time, event.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, url, time);
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", time='" + new Timestamp(time) + '\'' +
                '}';
    }


}
