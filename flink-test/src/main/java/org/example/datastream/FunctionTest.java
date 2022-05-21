package org.example.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * 自定义Function
 */
public class FunctionTest implements SetUp {
    /**
     * 不实用lambda表达式，自定义Function
     */
    @Test
    public void funcTest() throws Exception {
        data.filter(new MyFilterFunc("A")).print("Function");
        env.execute();
    }

    @Test
    public void richFuncTest() throws Exception {
        env.setParallelism(1);
        data.map(new MyMapRichFunc()).print("RichFunction");
        env.execute();
    }

    /**
     * 自定义Function重写方法，其实可以使用lambda表达式，使用这种方法的好处是不用return TypeHint类型，而且可以写更多复杂的逻辑，避免代码耦合
     */
    public static class MyFilterFunc implements FilterFunction<Event> {
        private final String keyWord;


        public MyFilterFunc(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event value) throws Exception {
            return value.getUser().contains(keyWord);
        }
    }

    /**
     * 自定义富函数，与常规函数不同的是富函数可以获取运行环境的上下文并拥有一些生命周期方法
     */
    public static class MyMapRichFunc extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("每一个并行的子任务开始的时候运行一次 " + getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Integer map(Event value) throws Exception {
            return value.getUser().length();
        }

        @Override
        public void close() throws Exception {
            System.out.println("每一个并行的子任务结束的时候运行一次" + getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
