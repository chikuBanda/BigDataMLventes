package org.mbds.hadoop.ventes;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomDoubleWritable implements Writable {
    private DoubleWritable double1;
    private DoubleWritable double2;

    public CustomDoubleWritable(){
        this.double1 = new DoubleWritable();
        this.double2 = new DoubleWritable();
    }

    public CustomDoubleWritable(double double1, double double2){
        this.double1 = new DoubleWritable(double1);
        this.double2 = new DoubleWritable(double2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.double1.write(dataOutput);
        this.double2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.double1.readFields(dataInput);
        this.double2.readFields(dataInput);
    }

    public DoubleWritable getDouble1() {
        return double1;
    }

    public void setDouble1(DoubleWritable double1) {
        this.double1 = double1;
    }

    public DoubleWritable getDouble2() {
        return double2;
    }

    public void setDouble2(DoubleWritable double2) {
        this.double2 = double2;
    }
}
