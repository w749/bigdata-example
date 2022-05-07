package org.mlamp.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.mlamp.mapreduce.phone.PhoneWritable;

/**
 * 自定义MapReduce分区，实现getPartition方法并在setPartitionerClass中指定
 */
public class MyPartitioner extends Partitioner<Text, PhoneWritable> {
    @Override
    public int getPartition(Text key, PhoneWritable value, int i) {
        String v = key.toString();
        if (v.startsWith("137")) {
            return 0;
        } else if (v.startsWith("135")) {
            return 1;
        } else if (v.startsWith("138")) {
            return 2;
        } else {
            return 0;
        }
    }
}
