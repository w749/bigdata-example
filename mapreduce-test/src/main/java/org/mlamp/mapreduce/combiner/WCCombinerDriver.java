package org.mlamp.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mlamp.mapreduce.wc.WordCountTest;

/**
 * 自定义Combiner
 * 如果没有Recue阶段（job.setNumReduceTasks(0)），那么从shuffle开始的包括Combiner和Reducer都不会执行
 */
public class WCCombinerDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WCCombinerDriver");
        job.setJarByClass(WCCombinerDriver.class);
        job.setMapperClass(WordCountTest.WordCountMapper.class);
        job.setReducerClass(WordCountTest.WordCountReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WCCombiner.class);  // 设置Combiner类，这里也可以直接使用Reducer类WordCountTest.WordCountReducer
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int res = job.waitForCompletion(true) ? 0 : 1;
    }
}
