package org.mlamp.mapreduce.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneComparableMapper extends Mapper<LongWritable, Text, PhoneComparable, Text> {
    private PhoneComparable outK = new PhoneComparable();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PhoneComparable, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        outV.set(split[0]);
        outK.setUp(Long.parseLong(split[1]));
        outK.setDown(Long.parseLong(split[2]));
        outK.setAll();
        System.out.println(outK + "," + outV);
        context.write(outK, outV);
    }
}
