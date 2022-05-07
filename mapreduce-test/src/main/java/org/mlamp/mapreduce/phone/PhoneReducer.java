package org.mlamp.mapreduce.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReducer extends Reducer<Text, PhoneWritable, Text, PhoneWritable> {
    private PhoneWritable upDown = new PhoneWritable();
    private Long up;
    private Long down;
    @Override
    protected void reduce(Text key, Iterable<PhoneWritable> values, Reducer<Text, PhoneWritable, Text, PhoneWritable>.Context context) throws IOException, InterruptedException {
        up = 0L;
        down = 0L;
        values.forEach(phone -> {
            up += phone.getUp();
            down += phone.getDown();
        });
        upDown.setUp(up);
        upDown.setDown(down);
        upDown.setAll();

        context.write(key, upDown);
    }
}
