package com.cht.mapreduce.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 自定义分组和排序，作为map输出的key
 */
public class ScoreBean implements WritableComparable<ScoreBean> {

    /*
    分数
     */
    private double score;

    /**
     * 科目
     */
    private String course;

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public ScoreBean(double score, String course) {
        this.score = score;
        this.course = course;
    }

    public ScoreBean() {
    }

    @Override
    public String toString() {
        return score + "\t" + course;
    }

    /**
     * 降序排列
     */
    @Override
    public int compareTo(ScoreBean o) {
        //首先进行按照课程进行排序，目的是到reduce端，所有的课程在一块
        int tmp = this.getCourse().compareTo(o.getCourse());

        //课程相同的时候才进行排序
        if (tmp == 0) {
            return o.getScore() - this.score > 0 ? 1 : (o.getScore() - this.score == 0 ? 0 : -1);
        }
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.score);
        out.writeUTF(this.course);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.score = in.readDouble();
        this.course = in.readUTF();
    }
}
