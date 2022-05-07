package org.mlamp.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCountTest {
    /**
     * 重写Mapper类的map方法
     * 默认使用的是TextInputFormat
     * Mapper<KEYIN（每一行输入数据的初识偏移量，固定LongWritable，Object也行）, VALUEIN（每一行输入数据的类型）, KEYOUT（输出数据的key类型）, VALUEOUT（输出数据的value类型）>
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text text = new Text();
        private IntWritable one = new IntWritable(1);

        /**
         * 执行map的准备方法，只运行一次
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * 写自己的业务逻辑
         * @param key 偏移量，对应Mapper中的KEYIN
         * @param value 当前行的数据，对应Mapper中的VALUEIN
         * @param context 可以获取Job定义的参数，与reduce和系统交互联系，通过context.write输出当前map的结果
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                text.set(stringTokenizer.nextToken());
                System.out.println("map: " + key + "\t" + text);
                context.write(text, one);
            }
        }

        /**
         * 清理阶段，map完成后执行一次
         */
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    /**
     * 重写Reducer的reduce方法
     * Reducer<Text（每个map输出的key）, IntWritable（所有map输出数据key相同的value迭代器）, Text（reduce key输出数据类型）, IntWritable（reduce value输出数据类型）>
     *     setUp方法和cleanUp方法和map的作用类似
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            AtomicInteger sum = new AtomicInteger();
            values.forEach(count -> sum.addAndGet(count.get()));
            result.set(sum.intValue());
            System.out.println("reduce: " + key.toString() + "\t" + result.toString());
            context.write(key, result);
        }
    }

    /**
     * Driver类，用来将Mapper和Reducer整合在一起并启动任务
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        // 初始化job，都是必须指定的项目，输入输出路径在Reducer输出为NullWritable时不用指定
        Job job = Job.getInstance(conf, "Test1");
        job.setJarByClass(WordCountTest.class);  // 指定主类
        job.setMapperClass(WordCountMapper.class);  // 指定Mapper类
        job.setReducerClass(WordCountReducer.class);  // 指定Reducer类
        job.setNumReduceTasks(1);  // 指定reduce数量，需要注意的是reduce数量不为1时会走HashPartitioner或者自定义Partitioner，为1时则不会走
        job.setOutputKeyClass(Text.class);  // 指定输出key类型
        job.setOutputValueClass(IntWritable.class);  //指定输出value类型
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  // 指定OutputFormat
        for (int i = 0; i < otherArgs.length - 1; ++i ) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));  // 输入路径，可以指定多个
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));  // 输出路径，必须为空文件夹
        boolean res = job.waitForCompletion(true);  // 指定任务并打印日志
        if (res) {
            System.out.println("Success");
            System.exit(0);
        } else {
            System.out.println("Fail");
            System.exit(0);
        }
    }
}
