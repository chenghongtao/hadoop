package com.cht.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hdfs 常用api
 */
public class HdfsApi {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.10:9000"), conf, "root");
        //上传
        //fs.copyFromLocalFile();

        //下载
        //fs.copyToLocalFile();

        //创建一级或多级文件夹
        //Path path=new Path("/mkdir");
        //Path path2=new Path("/test01/aa/bb/cc");
//        boolean mkdir1=fs.mkdirs(path);
        // boolean mkdir2=fs.mkdirs(path2);
//        System.out.println(mkdir1+":"+mkdir2);

        //删除文件夹
        //fs.delete(new Path("/test01"),true);

        //判断路径是否存在
        //fs.exists(path);

        //对文件进行重命名
        //fs.rename(oldname,newName);

        //获取指定目录下的文件列表
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            //每一个文件状态信息
            LocatedFileStatus status = iterator.next();
//            System.out.println(status);
//            //获取分块个数
//            System.out.println(status.getPath() + ":" + status.getBlockLocations().length);
//            //获取文件路径
//            System.out.println(status.getPath());
//            //获取文件大小
//            System.out.println(status.getLen());
//            //获取所属用户和所属组
//            System.out.println(status.getOwner() + ":" + status.getGroup());
//            //获取副本数
//            System.out.println(status.getReplication());
//            //获取块大小
//            System.out.println(status.getBlockSize());
//            //获取文件修改时间时间戳
//            System.out.println(status.getModificationTime());
//
            BlockLocation[] blocks = status.getBlockLocations();

            for (BlockLocation blk : blocks) {
                System.out.println(blk);
                //0,8371,hadoop01,hadoop02 起始偏移量，末尾偏移量 存在那个节点上
                //获取每一个块的存储节点
                System.out.println(blk.getHosts());
                //获取偏移量
                System.out.println(blk.getOffset());
            }
        }

        //获取指定目录或者文件的状态信息
        FileStatus[] status=fs.listStatus(new Path("/"));
        for(FileStatus file:status){
            System.out.println(file);
            //判断是否是目录
            System.out.println(file.isDirectory());
            //判断是否是文件
            System.out.println(file.isFile());
        }
    }
}
