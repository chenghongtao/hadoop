package com.cht.mapreduce.flow.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 该类是一个普通的实体类，并且做了hadoop的序列化，
 *
 * 实现Writable接口，实现write（），readFields()方法
 */
public class FlowBean implements Writable {

    private Long upFlow;

    private Long downFlow;

    private Long totalFlow;

    public FlowBean() {
    }

    public FlowBean(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(Long totalFlow) {
        this.totalFlow = totalFlow;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.totalFlow);
    }

    //反序列化：注意序列化和反序列化字段的对应的顺序
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.totalFlow = in.readLong();
    }
}
