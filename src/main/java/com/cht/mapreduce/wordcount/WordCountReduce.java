package com.cht.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 合并阶段
 * KEYIN：是map阶段输出的的keyout：String
 * VALUEIN：是map阶段函数的valueout：int
 * KEYOUT：是某一组单词，所以类型是String
 * VALUEOUT：某组单词的个数，所以类型是Int
 *
 *
 *
 * 框架在map处理完成之后,将所有的KV对保存起来,进行分组,然后传递一个组,调用一次reduce
 * 相同的key在一个组
 * reduce接收到的数据是：
 *     <hello,1,1,1,1.......><hadoop,1,1,1,1.......>
 *     <spark,1,1,1,1.......> <hive,1,1,1,1.......>
 *     将次数累加求和，再输出
 *
 * 每组数据执行一次
 *
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //累加统计
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }

        //输出一组(一个单词)的统计结果
        ///默认输出到HDFS的一个文件上面去,放在HDFS的某个目录下
        context.write(key, new IntWritable(sum));
    }
}
