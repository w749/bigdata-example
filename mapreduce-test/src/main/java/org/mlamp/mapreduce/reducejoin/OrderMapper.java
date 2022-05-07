package org.mlamp.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
    private Text text = new Text();
    private OrderBean order = new OrderBean();
    private String fileName;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, OrderBean>.Context context) throws IOException, InterruptedException {
        fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if ("joinOrder".equals(fileName)) {
            text.set(split[1]);
            order.setOrderID(split[0]);
            order.setOrderAmount(Integer.parseInt(split[2]));
            order.setOrderName("");
        } else {
            text.set(split[0]);
            order.setOrderID("");
            order.setOrderAmount(0);
            order.setOrderName(split[1]);
        }
        order.setFileName(fileName);
        context.write(text, order);
    }
}
