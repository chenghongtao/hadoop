package com.cht.mapreduce.inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MergeFileJob {

    /**
     * 自定义输入key
     * 自定义输出value
     * 输出到reduce方法key
     * 输出到reduce方法value
     */
    static class MyMapper extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    static class MyReduce extends Reducer<Text,Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text text : values){
                context.write(text,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();

        System.setProperty("HADOOP_USER_NAME","root");

        conf.set("fs.defaultFS","hdfs://hadoop01:9000");

        Job job= Job.getInstance(conf);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指定自定义输入
        job.setInputFormatClass(MergeFileInputFormat.class);

        FileInputFormat.addInputPath(job,new Path("/hadoop/merge"));

        FileOutputFormat.setOutputPath(job,new Path("/hadoop/mergeout"));

        boolean b=job.waitForCompletion(true);

        System.exit(b?0:1);
    }
}
