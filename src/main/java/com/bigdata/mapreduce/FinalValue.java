package com.bigdata.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FinalValue implements Writable {
    private Long totalTransactions;
    private Double averagePrice;
    private Long epochDiff;

    public FinalValue(){
        totalTransactions = 0L;
        averagePrice = 0D;
        epochDiff = 0L;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(totalTransactions);
        dataOutput.writeDouble(averagePrice);
        dataOutput.writeLong(epochDiff);
    }

    public void readFields(DataInput dataInput) throws IOException {
        totalTransactions = dataInput.readLong();
        averagePrice = dataInput.readDouble();
        epochDiff = dataInput.readLong();
    }

    public Long getTotalTransactions() {
        return totalTransactions;
    }

    public void setTotalTransactions(Long totalTransactions) {
        this.totalTransactions = totalTransactions;
    }

    public Double getAveragePrice() {
        return averagePrice;
    }

    public void setAveragePrice(Double averagePrice) {
        this.averagePrice = averagePrice;
    }

    public Long getEpochDiff() {
        return epochDiff;
    }

    public void setEpochDiff(Long epochDiff) {
        this.epochDiff = epochDiff;
    }

    @Override
    public String toString(){
        return String.valueOf(averagePrice) +
                ',' +
                totalTransactions +
                ',' +
                epochDiff;
    }
}
