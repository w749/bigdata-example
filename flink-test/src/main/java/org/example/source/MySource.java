package org.example.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.example.util.Event;

import java.util.Random;

/**
 * 自定义SourceFunction发送对象
 * ParallelSourceFunction相比SourceFunction接口可以增加并行度
 */
public class MySource implements ParallelSourceFunction<Event> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String user;
        String url;
        String[] users = {"Bob", "Alice", "Friedman", "Angil"};
        String[] urls = {"./123", "./abc", "./321", "/efg"};

        // 循环随机生成数据
        while (running) {
            user = users[random.nextInt(users.length)];
            url = urls[random.nextInt(urls.length)];
//            ctx.collect(new Event(user, url, System.currentTimeMillis()));  // 只发送数据
            // 带事件时间发送数据，并且发送Watermark
            ctx.collectWithTimestamp(new Event(user, url, System.currentTimeMillis()), System.currentTimeMillis());
            ctx.emitWatermark(new Watermark(System.currentTimeMillis() -1L));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
