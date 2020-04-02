package com.cht.mapreduce.wordcount.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private int upFlow;

    private int downFlow;

    private int totalFlow;

    public FlowBean() {
    }

    public FlowBean(int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(int totalFlow) {
        this.totalFlow = totalFlow;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.write(upFlow);
        out.write(downFlow);
        out.write(totalFlow);
    }

    //反序列化：注意序列化和反序列化字段的对应的顺序
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.totalFlow = in.readInt();
    }
}
