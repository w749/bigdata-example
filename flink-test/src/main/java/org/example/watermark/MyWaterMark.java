package org.example.watermark;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.example.util.Event;

/**
 * 自定义Watermark，没写逻辑
 */
public class MyWaterMark implements WatermarkStrategy<Event> {
    /**
     * 生成Watermark策略
     * @param context
     * @return
     */
    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyWatermarkGenerator();
    }

    public static class MyWatermarkGenerator implements WatermarkGenerator<Event> {

        /**
         * 每一条数据都会调用一次，更新最大时间戳
         * 当要生成断点式水位线生成器时（因为onEvent每一条数据都会调用一次，遇到特殊数据就可以直接标记Watermark）
         *     使用onEvent的output标记Watermark，此时就不调用onPeriodicEmit来标记了
         * @param event 当前事件
         * @param eventTimestamp 当前事件的时间戳
         * @param output 用来标记Watermark
         */
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {

        }

        /**
         * 在这里发出Watermark，由系统周期性的调用，默认200ms
         * @param output 标记Watermark
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }

}
