package org.mlamp.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Locale;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FileSystem fileSystem;
    private FSDataOutputStream outA;
    private FSDataOutputStream outB;

    /**
     * 首先要根据context获取到FileSystem，根据它创建输出流
     */
    public LogRecordWriter(TaskAttemptContext context) {
        try {
            fileSystem = FileSystem.get(context.getConfiguration());
            outA = fileSystem.create(new Path("/Users/mlamp/workspace/my/test-utils/mapreduce-test/data/output/6/A.log"), true);
            outB = fileSystem.create(new Path("/Users/mlamp/workspace/my/test-utils/mapreduce-test/data/output/6/B.log"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 这里是每一条数据需要调用的写出到文件的方法，在这里做判断写出到不同的流
     */
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String str = text.toString();
        if (str.toUpperCase().contains("A")) {
            outA.writeBytes(str + "\n");
        } else {
            outB.writeBytes(str + "\n");
        }
    }

    /**
     * 这里来关闭输出流
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(outA);
        IOUtils.closeStream(outB);
    }
}
