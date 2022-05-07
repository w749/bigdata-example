package org.mlamp.mapreduce.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements Writable {
    private String orderID;
    private int orderAmount;
    private String orderName;
    private String fileName;

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public void setOrderAmount(int orderAmount) {
        this.orderAmount = orderAmount;
    }

    public void setOrderName(String orderName) {
        this.orderName = orderName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public String getOrderID() {
        return orderID;
    }

    public int getOrderAmount() {
        return orderAmount;
    }

    public String getOrderName() {
        return orderName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderID);
        dataOutput.writeInt(orderAmount);
        dataOutput.writeUTF(orderName);
        dataOutput.writeUTF(fileName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderID = dataInput.readUTF();
        orderAmount = dataInput.readInt();
        orderName = dataInput.readUTF();
        fileName = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return orderAmount + "\t" + orderName;
    }
    public String toString1() {
        return orderID + "\t" +  orderAmount + "\t" + orderName + "\t" + fileName;
    }
}
