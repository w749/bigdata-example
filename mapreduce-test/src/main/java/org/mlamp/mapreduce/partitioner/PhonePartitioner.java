package org.mlamp.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mlamp.mapreduce.phone.PhoneWritable;
import org.mlamp.mapreduce.phone.PhoneMapper;
import org.mlamp.mapreduce.phone.PhoneReducer;


public class PhonePartitioner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PhoneTest1");
        job.setJarByClass(PhonePartitioner.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);

        // 指定分区数和分区类
        job.setNumReduceTasks(3);  // 注意这个数量必须和自定义Partitioner规则的数量相同，1除外，它不会走自定义Partitioner
        job.setPartitionerClass(MyPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int res = job.waitForCompletion(true) ? 1 : 0;
    }
}
