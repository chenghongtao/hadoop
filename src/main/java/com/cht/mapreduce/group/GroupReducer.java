package com.cht.mapreduce.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 接收已经分组，排序完成的数据
     * computer 98
     * computer 96
     * computer 88
 * 输出科目，姓名，平均分
 */
public class GroupReducer extends Reducer<ScoreBean, Text,Text, NullWritable> {

    @Override
    protected void reduce(ScoreBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          //获取第一个学生的信息
          Text name=values.iterator().next();
          context.write(new Text(name.toString()+"\t"+key.toString()),NullWritable.get());
    }
}
