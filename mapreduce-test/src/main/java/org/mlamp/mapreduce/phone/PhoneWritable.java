package org.mlamp.mapreduce.phone;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义Mapper的value时需要实现Writable中的write和readFields方法
 * 注意这两个方法的顺序必须一致
 */
public class PhoneWritable implements Writable {
    private Long up;
    private Long down;
    private Long all;

    public Long getUp() {
        return up;
    }

    public Long getDown() {
        return down;
    }

    public Long getAll() {
        return all;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public void setAll(Long all) {
        this.all = all;
    }

    public void setAll() {
        this.all = this.up + this.down;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(all);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        up = in.readLong();
        down = in.readLong();
        all = in.readLong();
    }

    @Override
    public String toString() {
        return up + "\t" + down + "\t" + all;
    }
}
