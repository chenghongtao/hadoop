package com.cht.mapreduce.inputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.zookeeper.txn.Txn;

import java.io.IOException;

/**
 * 进行文件读取：
 *    整个文件进行读取
 */
public class MergeFileRecorderReader extends RecordReader<Text, Text> {

    //文件输入流
    private FSDataInputStream in=null;

    //标识文件是否读取完成
    private boolean isFinshed=false;

    //文件长度
    private long size=0;

    //文件内容value
    private Text value=new Text();

    //key 文件名称
    private Text key=value=new Text();

    /**
     *
     * @param split  文件切片
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit=(FileSplit)split;
        Path path=fileSplit.getPath();

        //获取流
        FileSystem fs=FileSystem.get(context.getConfiguration());
        in = fs.open(path);

        //获取文件长度
        size=fileSplit.getLength();

        //设置文件名称
        key.set(fileSplit.getPath().toString());

    }

    //文件是否读取结束，这块不正常结束可能造成死循环
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isFinshed){
             byte[] bytes=new byte[(int)size];
             in.readFully(0,bytes);
             value.set(bytes);
             isFinshed=true;
             return isFinshed;
        }else {
            return false;
        }

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    //获取执行进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isFinshed?1.0F:0.0F;
    }

    //关闭资源
    @Override
    public void close() throws IOException {
          in.close();
    }
}
