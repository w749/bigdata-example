package org.mlamp.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.mlamp.mapreduce.reducejoin.OrderBean;

import java.net.URI;

/**
 * 使用MapJoin，不经过Reduce避免数据倾斜
 * 将数据量较小的直接缓存到内存中每个map从内存中获取数据并join后直接输出结果
 */
public class OrderMapJoinDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "OrderMapJoinDriver");
        job.setJarByClass(OrderMapJoinDriver.class);
        job.setMapperClass(OrderMapJoinMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OrderBean.class);
        job.addCacheFile(new URI(args[0]));  // 缓存数据到内存
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        int res = job.waitForCompletion(true) ? 0 : 1;
    }
}
