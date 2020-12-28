package org.shanks;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class customReturnValueWritable implements Writable {
    private IntWritable quantityOnline;
    private IntWritable quantityOffline;
    private DoubleWritable totalProfit;

    public customReturnValueWritable() {
        quantityOffline = new IntWritable();
        quantityOnline = new IntWritable();
        totalProfit = new DoubleWritable();
    }

    public customReturnValueWritable(IntWritable quantityOnline,
                                     IntWritable quantityOffline,
                                     DoubleWritable totalProfit) {
        this.quantityOnline = quantityOnline;
        this.quantityOffline = quantityOffline;
        this.totalProfit = totalProfit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        quantityOffline.write(dataOutput);
        quantityOnline.write(dataOutput);
        totalProfit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        quantityOnline.readFields(dataInput);
        quantityOffline.readFields(dataInput);
        totalProfit.readFields(dataInput);
    }

    public IntWritable getQuantityOnline() {
        return quantityOnline;
    }

    public void setQuantityOnline(IntWritable quantityOnline) {
        this.quantityOnline = quantityOnline;
    }

    public IntWritable getQuantityOffline() {
        return quantityOffline;
    }

    public void setQuantityOffline(IntWritable quantityOffline) {
        this.quantityOffline = quantityOffline;
    }

    public DoubleWritable getTotalProfit() {
        return totalProfit;
    }

    public void setTotalProfit(DoubleWritable totalProfit) {
        this.totalProfit = totalProfit;
    }
}
