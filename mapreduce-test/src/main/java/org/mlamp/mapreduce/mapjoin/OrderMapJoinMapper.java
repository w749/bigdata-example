package org.mlamp.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mlamp.mapreduce.reducejoin.OrderBean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class OrderMapJoinMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
    private Text text = new Text();
    private OrderBean order = new OrderBean();
    private HashMap<String, String> product = new HashMap();

    /**
     * 将Driver缓存的数据读取到HashMap中
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, OrderBean>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream stream = fs.open(new Path(cacheFiles[0]));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        String line;
        String[] split;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            split = line.split(",");
            product.put(split[0], split[1]);
        }
        IOUtils.closeStream(stream);
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        text.set(split[1]);
        order.setOrderID(split[0]);
        order.setOrderAmount(Integer.parseInt(split[2]));
        order.setOrderName(product.get(split[1]));
        context.write(text, order);
    }
}
