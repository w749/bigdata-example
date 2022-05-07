package org.mlamp.mapreduce.comparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneComparable implements WritableComparable<PhoneComparable> {
    private Long up;
    private Long down;
    private Long all;

    public void setUp(Long up) {
        this.up = up;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public void setAll() {
        this.all = this.up + this.down;
    }

    public Long getUp() {
        return up;
    }

    public Long getDown() {
        return down;
    }

    public Long getAll() {
        return all;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(all);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.all = dataInput.readLong();
    }

    @Override
    public int compareTo(PhoneComparable other) {
        return this.all > other.getAll() ? -1 : 1;
    }

    @Override
    public String toString() {
        return up + "\t" + down + "\t" + all;
    }
}
