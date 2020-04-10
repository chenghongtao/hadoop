package com.cht.mapreduce.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计文件总共有多少行,还有单词数目
 */
public class MyJob {

    static class MyMapper extends Mapper<LongWritable, Text,NullWritable, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] dates=value.toString().split("\t");
             context.getCounter(MyCounter.LINES).increment(1L);
             context.getCounter(MyCounter.WORDS).increment(dates.length);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();

        //跨平台运行
        conf.set("mapreduce.app-submission.cross-platform", "true");

        //设置用户名
        System.setProperty("HADOOP_USER_NAME","root");

        Job job=Job.getInstance(conf);

        job.setJarByClass(MyJob.class);

        job.setMapperClass(MyMapper.class);

        job.setMapOutputValueClass(NullWritable.class);

        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/counter"));

        job.waitForCompletion(true);

    }
}
