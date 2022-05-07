package org.mlamp.mapreduce.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 实现使用自定义类作为Mapper的key，自定义类必须同时实现Writable和Comparable接口并重写方法
 * 注意必须指定setMapOutputKeyClass和setMapOutputValueClass
 */
public class PhoneComparableDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PhoneComparableDriver");
        job.setJarByClass(PhoneComparableDriver.class);
        job.setMapperClass(PhoneComparableMapper.class);
        job.setReducerClass(PhoneComparableReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(PhoneComparable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneComparable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

       int res = job.waitForCompletion(true) ? 1 : 0;
    }
}
