package com.cht.mapreduce.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 求每门课程参考学生的成绩最高平均分的学生信息  也就是topN问题
 *
 * 思路：
 *   map发出的key是科目+分数    默认科目和分数相同的分为一组 ，则要自定义
 *   map发出的value是学生姓名
 *
 */
public class MyGroup extends WritableComparator {

    public MyGroup(){
        //默认情况下第二个参数为false，不回构建示例对象，所以要传成false
        super(ScoreBean.class,true);
    }

    //分组方法
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        
        //只返回为0的值
        ScoreBean asb=(ScoreBean)a;
        ScoreBean bsb=(ScoreBean)b;
        return asb.getCourse().compareTo(bsb.getCourse());
    }
}
