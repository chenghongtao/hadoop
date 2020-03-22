package com.cht.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用IO流的方式操作
 */
public class HdfsIO {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.10:9000"), conf, "root");

        //文件上传(本地到hdfs)
        //本地输入流    hdfs输出流
//        FileInputStream in = new FileInputStream(new File("E:\\softwoare\\apache-maven-3.6.0-bin.zip"));
//        FSDataOutputStream out = fs.create(new Path("/hadoop/aa.zip"));
//        //:文件复制输出流 输出流 缓冲区大小
//        IOUtils.copyBytes(in,out,4096);


        //文件下载  hdfs（hdfs输入流）-》本地（输出流）
        //hdfs输入流
        FSDataInputStream in1=fs.open(new Path("/hadoop/aa.zip"));
        //本地输出零六
        FileOutputStream out1=new FileOutputStream(new File("d:\\bb.zip"));
        //复制
        IOUtils.copyBytes(in1,out1,4096);

    }
}
