package com.cht.mapreduce.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计文件平均分，将平均分小于60和大于=60的输入到不同的文件中
 */
public class DiffFileJob {

      static class DiffMapper extends Mapper<LongWritable,Text, Text, DoubleWritable>{

          private Text k= new Text();

          private DoubleWritable v=new DoubleWritable();


          @Override
          protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                 String[] datas=value.toString().split(",");
                 k.set(datas[0]);
                 int score=0;
                 int count=0;
                 for(int i=1;i<datas.length;i++){
                     score+=Integer.valueOf(datas[i]);
                     count++;
                 }

                 double avg=score/count;

                 v.set(avg);

                 context.write(k,v);

          }
      }

      static class DiffReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

          @Override
          protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
              context.write(key,values.iterator().next());
          }
      }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();

        System.setProperty("HADOOP_USER_NAME","root");

        conf.set("fs.defaultFS","hdfs://hadoop01:9000");

        Job job= Job.getInstance(conf);

        job.setJarByClass(DiffFileJob.class);

        job.setReducerClass(DiffReducer.class);

        job.setMapperClass(DiffMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //指定自定义outputformat
        job.setOutputFormatClass(DiffFileOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path("/hadoop/score"));

        //存放执行成功数据文件
        FileOutputFormat.setOutputPath(job,new Path("/hadoop/success"));

        boolean b=job.waitForCompletion(true);

        System.exit(b?0:1);
    }
}
