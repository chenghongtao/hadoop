package com.cht.mapreduce.mapper.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class MapJoin {
    static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{

        //存放小表数据
        private Map<String,String> map=new HashMap<>();

        //
        private Text text=new Text();

        //在maptask之前，将小表数据添加到内存中
        /*订单数据字段：
         *    id    date  pid   amount
         * 商品信息表
         *    pid   name    category_id price
         */
        @Override
        protected void setup(Context context) throws IOException {
            Path cacheFile = context.getLocalCacheFiles()[0];
            String path=cacheFile.getName();
            BufferedReader br=new BufferedReader(new FileReader(path));
            String line=null;
            while((line=br.readLine())!=null){
                String[] strings = line.split("\t");
                String pid=strings[0];
                String elseInfo=strings[1]+strings[2];
                map.put(pid,elseInfo);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] strs=value.toString().split("\t");
             String pid=strs[2];
             if(map.containsKey(pid)){
                 String productInfo=map.get(pid);
                 String result=productInfo+"\t"+value.toString();

                 //将值添加到容器中
                 text.set(result);
                 context.write(text,NullWritable.get());
             }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Configuration conf=new Configuration();

            conf.set("fs.defaultFS","hdfs://hadoop01:9000");

            System.setProperty("HADOOP_USER_NAME","root");

            Job job= Job.getInstance(conf);

            job.setMapperClass(MyMapper.class);
            //如果没有reduce，则需要设置reducetask任务为0
            job.setNumReduceTasks(0);

            //Mapper的输出就是结果的输出
            job.setMapOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(Text.class);

            //将文件加载到缓冲区中
            job.addCacheFile(new URI("/hadoop/cache"));

            FileInputFormat.addInputPath(job,new Path("/hadoop/join/mapper"));

            FileOutputFormat.setOutputPath(job,new Path("/hadoop/join/mapout"));

            boolean b=job.waitForCompletion(true);

            System.exit(b?0:1);

        }
    }
}
