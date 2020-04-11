package com.cht.mapreduce.reduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoin {
    /**
     * 输出：
     *     key：关联字段
     *     value：打标记的数据
     */
    static class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,Text>{

        private String fileName="";

        private Text outKey=new Text();

        private Text outValue=new Text();

        //在maptask程序执行前，只执行一次，通常用于资源初始化之类的功能
        @Override
        protected void setup(Context context)  {

            //获取文件切片信息
            FileSplit split= (FileSplit) context.getInputSplit();
            //获取文件名称
            this.fileName =split.getPath().getName();
        }

        /**
         *
         * 订单数据字段：
         *    id    date  pid   amount
         * 商品信息表
         *    pid   name    category_id price
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] arr=value.toString().split("\t");

             //命名两个文件名称分别为：order.txt product.txt
             //区分不同的数据，打标记
             if("order.txt".equals(fileName)){
                outKey.set(arr[2]);
                outValue.set("OR"+arr[0]+"\t"+arr[1]+"\t"+arr[3]);
                context.write(outKey,outValue);
             }else{
                 outKey.set(arr[0]);
                 outValue.set("PR"+arr[1]+"\t"+arr[2]+"\t"+arr[3]);
                 context.write(outKey,outValue);
             }
        }

        //maptask 任务完成后执行一次 用于释放资源
        protected void cleanup(Context context)  {
            // NOTHING
        }
    }


    static class ReduceJoinReducer extends Reducer<Text,Text,Text, NullWritable>{
        private List<String> orders=new ArrayList<>();
        private List<String> products=new ArrayList<>();

        Text resultText=new Text();

        //发送过来的数据
        //pid order1 order2 product   订单对商品，多对一
        //拼接数据
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

             //接收数据
             for(Text value : values){
                 String str=value.toString();
                 if(str.startsWith("PO")){
                     products.add(str);
                 }else{
                     orders.add(str);
                 }
             }

             //拼接数据
            for (String pro:products){
                String proStr=pro.substring(2);
                for(String order : orders){
                    String orderStr=order.substring(2);
                    String result=proStr+"\t"+orderStr;
                    resultText.set(result);
                    context.write(resultText,NullWritable.get());
                }
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();

        System.setProperty("HADOOP_USER_NAME","root");

        Job job= Job.getInstance(conf);

        job.setJarByClass(ReduceJoin.class);

        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/join/reduce"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.2.10:9000/hadoop/join/reduceout01"));

        boolean b=job.waitForCompletion(true);

        System.exit(b?0:1);
    }
}
