package com.cht.mapreduce.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 将课程和分数封装为对象，自定义排序
 */
public class MyMapper extends Mapper<LongWritable, Text,ScoreBean,Text> {

    private ScoreBean bean=new ScoreBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas=value.toString().split(",");
        String course=datas[0];
        String name=datas[1];
        
        //求平均值
        int count=0;
        int sum=0;
        for(int i=2;i<datas.length;i++){
            count++;
            sum+=Integer.valueOf(datas[i]);
        }

        double avg=sum/count;
        bean.setScore(avg);
        bean.setCourse(course);
        context.write(bean,new Text(name));
    }
}
