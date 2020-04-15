package com.cht.mapreduce.outputFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DiffFileOutputFormat extends FileOutputFormat<Text, DoubleWritable> {


    //获取RecordWriter对象
    @Override
    public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new DiffFileRecordWriter(job);
    }
}
