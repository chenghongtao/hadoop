package com.cht.mapreduce.flow.statistics;

import com.cht.mapreduce.flow.model.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().split("\t");
        FlowBean bean=new FlowBean(Long.valueOf(strs[1]),Long.valueOf(strs[2]));
        context.write(new Text(strs[0]),bean);
    }
}
