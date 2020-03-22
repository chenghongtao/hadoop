package com.cht.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsTest {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.10:9000"), conf,"root");
        fs.copyFromLocalFile(new Path("D:\\java\\javafx-src.zip"), new Path("/"));
    }
}
