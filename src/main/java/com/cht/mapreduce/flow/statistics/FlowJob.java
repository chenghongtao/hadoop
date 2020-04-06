package com.cht.mapreduce.flow.statistics;

import com.cht.mapreduce.statistics.model.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf=new Configuration();

        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job=Job.getInstance(conf);

        job.setJarByClass(FlowJob.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/test"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/outFlow"));

        boolean b=job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
