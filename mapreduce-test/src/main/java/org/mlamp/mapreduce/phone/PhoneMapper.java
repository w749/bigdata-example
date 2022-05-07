package org.mlamp.mapreduce.phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, Text, PhoneWritable> {
    private String[] line;
    private Text phone = new Text();
    private PhoneWritable upDown = new PhoneWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneWritable>.Context context) throws IOException, InterruptedException {
        line = value.toString().split(",");
        phone.set(line[0]);
        upDown.setUp(Long.parseLong(line[2]));
        upDown.setDown(Long.parseLong(line[3]));
        upDown.setAll();

        context.write(phone, upDown);
    }
}
