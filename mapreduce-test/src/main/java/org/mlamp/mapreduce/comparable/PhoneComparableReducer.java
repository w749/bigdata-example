package org.mlamp.mapreduce.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneComparableReducer extends Reducer<PhoneComparable, Text, Text, PhoneComparable> {
    @Override
    protected void reduce(PhoneComparable key, Iterable<Text> values, Reducer<PhoneComparable, Text, Text, PhoneComparable>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
