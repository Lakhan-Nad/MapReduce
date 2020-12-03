package com.bigdata.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntermediateValue implements Writable{
    private Long epochTime;
    private Double price;

    public IntermediateValue(){
        epochTime = 0L;
        price = 0D;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(price);
        dataOutput.writeLong(epochTime);
    }

    public void readFields(DataInput dataInput) throws IOException {
        price = dataInput.readDouble();
        epochTime = dataInput.readLong();
    }

    public Long getEpochTime() {
        return epochTime;
    }

    public void setEpochTime(Long epochTime) {
        this.epochTime = epochTime;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
