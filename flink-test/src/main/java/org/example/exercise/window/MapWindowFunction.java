package org.example.exercise.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.util.Event;

/**
 * 将数据包装为为Event
 */
public class MapWindowFunction implements MapFunction<String, Event> {
    @Override
    public Event map(String value) throws Exception {
        String[] split = value.split(",");
        return new Event(split[0], split[1], Long.parseLong(split[2]));
    }
}
