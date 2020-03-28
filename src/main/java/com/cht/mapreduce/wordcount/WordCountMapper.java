package com.cht.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN ： 是指框架读取到数据key的类型，在默认的InputFormat下，读到的key是一行文本的起始偏移量，所以key的类型为Long
 * VALUEIN ：是指框架读取到的数据的value的类型，读到的内容是一行的文本内容，所以value的类型是String
 * KEYOUT：是指用户自定义逻辑方法返回的数据中key的类型，有业务逻辑决定，在wordcount中，输出的key是单词，所以是String类型
 * VALUEOUT：是指用户自定义逻辑方法返回的数据中value的类型，由业务逻辑决定，在wordcount中，输出的value是单词的数量，所以是Integer类型
 *
 * 序列化和反序列化：只要程序中数据要存入磁盘或者经过网络传输，必须经过序列化和反序列化。
 *
 *
 *
 * 但是String，Long等JDK中自带的数据类型,在序列化中，连通类结构也序列化了，hadoop要的只是其中的数据，
 * 所以自定义了一套序列化框架，所以在hadoop中，如果该数据需要序列化(写磁盘或者网络传输)，
 * 就一定要使用hadoop自定义的序列化类型
 *
 * 基本类型：XXXWritable
 * String：Text
 * Null：NullWriteable
 *
 *
 * 该map函数是读一行调用一次
 *
 * map输出的结果应该是<hello,1><word,1> <hadoop,><spark,1> <hive,1><hadoop,1> <hello,1> <hello,1>
 *
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words=value.toString().split("\t");
        //输出就是的是这样的<hello,1> <hadoop,1> <hello,1> <spark,1> <spark,1> <hadoop,1>
        for(String word : words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
