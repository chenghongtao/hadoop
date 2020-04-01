package com.cht.mapreduce.wordcount.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 定义分区规则(a-f 放一个文件  f-p放一个文件 q-z放一个文件)
 * @param <Text>
 * @param <IntWritable>
 */
public class MyPartitioner<Text, IntWritable> extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String str=text.toString();
        char ch=str.charAt(0);
        if(ch>='a' && ch<'f'){
            return 0;
        }else if(ch>='f' && ch <'p'){
            return 1;
        }else{
            return 2;
        }
    }
}
