package com.cht.mapreduce.wordcount.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
   Combiner是在每个节点先做的一个小范围的reduce，目的是减少shuffle阶段和reduce阶段的数据量，减小reduce阶段的压力。
   1.实现Reducer类
   2.在job中使用job.setCombinerClass(WordCountReduce.class)方法指定,
   注意：一般combiner和reduce的逻辑一样，所以直接指定自定义的reduce类即可。
 */

public class MyCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    }
}
