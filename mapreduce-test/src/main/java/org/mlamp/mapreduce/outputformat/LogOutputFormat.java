package org.mlamp.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义OutputFormat
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {

    private LogRecordWriter logWriter;

    /**
     * 重写这个方法并返回RecordWriter对象来写出每一次接收的数据
     * @param context 这里是Reducer的Context
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        logWriter = new LogRecordWriter(context);
        return logWriter;
    }
}
