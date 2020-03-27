package com.cht.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 删除hdfs上 给定目录的所有空目录
 */
public class DeleteHDFSEmptyDirAndFile {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Path path = new Path("/");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.10:9000"), conf, "root");
        deleteEmptyDir(fs, path);

        //最后判断给定路径是否为空(也就是底下空目录空文件删除完成，给定目录是否为空)
        FileStatus[] fileStatuses=fs.listStatus(path);
        if(fileStatuses.length==0){
            fs.delete(path,false);
        }
    }

    public static void deleteEmptyDir(FileSystem fs, Path path) throws IOException {

        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatuse : fileStatuses) {

            Path p1 = fileStatuse.getPath();
            //如果是文件，则判断大小是否为0
            if (fileStatuse.isFile() && fileStatuse.getLen() == 0) {
                fs.delete(p1, false);
            }

            if (fileStatuse.isDirectory()) {
                deleteEmptyDir(fs, p1);
            }
        }
    }

}
