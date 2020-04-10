package com.cht.mapreduce.group;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 求每门课程参考学生的成绩最高平均分的学生信息  也就是topN问题
 *
 * 思路：
 *   map发出的key是科目+分数    默认科目和分数相同的分为一组 ，则要自定义
 *    排序：按照分数排序
 *    分组：按照科目分组
 *    map发出的value是学生姓名
 *
 */
public class GroupJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        System.setProperty("HADOOP_USER_NAME","root");
        Job job= Job.getInstance(conf);
        job.setJarByClass(GroupJob.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(GroupReducer.class);

        job.setMapOutputValueClass(ScoreBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);

        //指定分组类
        job.setGroupingComparatorClass(MyGroup.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/group"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/group/out01"));

        boolean b=job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
