package com.cht.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 删除hdfs中给定目录下给定文件类型的所有文件
 */
public class DeleteHDFSFileByType {

    private static final String FILE_TYPE = ".class";

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        Path path = new Path("/");
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.10:9000"), conf, "root");
        deleteHDFSFileByType(fs, path);
    }

    public static void deleteHDFSFileByType(FileSystem fs, Path path) throws IOException {
        FileStatus[] list = fs.listStatus(path);

        //目录为空，直接退出
        if (list.length == 0) {
            return;
        }

        for (FileStatus file : list) {

            Path filePath = file.getPath();

            //如果是文件并且文件类型是指定文件，则删除
            if (file.isFile() && filePath.getName().endsWith(FILE_TYPE)) {
                System.out.println("删除的文件名称------------" + filePath.getParent()+"/"+filePath.getName());
                fs.delete(path, false);
            }

            //如果是目录，则取下一层
            if (file.isDirectory()) {
                deleteHDFSFileByType(fs, filePath);
            }

        }

    }

}
