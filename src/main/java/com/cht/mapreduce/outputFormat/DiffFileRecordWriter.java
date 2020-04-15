package com.cht.mapreduce.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DiffFileRecordWriter extends RecordWriter<Text, DoubleWritable> {

    //大于等于60分的输出流
    private FSDataOutputStream more;

    //小于60分的输出流
    private FSDataOutputStream less;


    public DiffFileRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        more = fs.create(new Path("/hadoop/difffile/out01"));
        less = fs.create(new Path("/hadoop/difffile/out02"));
    }

    //写方法
    @Override
    public void write(Text key, DoubleWritable value) throws IOException, InterruptedException {
        if(value.get()>=60){
            more.write((key+"\t"+value+"\n").getBytes());
        }else{
            less.write((key+"\t"+value+"\n").getBytes());
        }

    }

    //关闭资源方法
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        more.close();
        less.close();
    }
}
