package org.mlamp.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mlamp.mapreduce.phone.PhoneMapper;
import org.mlamp.mapreduce.phone.PhoneReducer;
import org.mlamp.mapreduce.phone.PhoneWritable;

public class PhoneCompress {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 设置mapper输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.setClass("mapreduce.map.output.compress.codecs", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf, "PhoneCompress");
        job.setJarByClass(PhoneCompress.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置reduce输出压缩
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        int res = job.waitForCompletion(true) ? 1 : 0;
    }
}
