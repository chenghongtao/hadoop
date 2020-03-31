package com.cht.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 用来描述一个特定的作业：
 *
 * 该作业类使用哪个类作为逻辑处理的map
 * 使用哪个类作为reduce
 * 要处理的数据在哪个路径
 * 结果输出存放在哪个的路径
 */
public class WordCount  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //首先要描述一个作业类，哪个map，哪个reduce，输入输出路径是什么   ----------Job对象
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        //设置驱动类
        job.setJarByClass(WordCount.class);

        //指定map类和输出key和输出value的类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //指定reduce和输出key和value的类型
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置切片大小 330 byte
        //FileInputFormat.setMaxInputSplitSize(job,330);
        //FileInputFormat.setMaxInputSplitSize(job,330);

        //合并文件进行maptask任务划分
        //job.setInputFormatClass(CombineFileInputFormat.class);
        //设置为10M，切片大小设置为10M
        //CombineFileInputFormat.setMaxInputSplitSize(job,10*1024*1024);
        //添加输入路径
        //CombineFileInputFormat.addInputPaths(job,"/wordcount/in");

        //指定要处理的数据路径
        //这个路径下的所有文件都会去读的
        FileInputFormat.addInputPaths(job,"/wordcount/in");


        //指定处理结果存放路径,这个文件目录一定要不存在
        FileOutputFormat.setOutputPath(job,new Path("/wordcount/out"));

        //提交任务到集群
        // 参数含义：true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        //          为true的话可以获取map task和 reduce task 任务的进度
        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }

}
