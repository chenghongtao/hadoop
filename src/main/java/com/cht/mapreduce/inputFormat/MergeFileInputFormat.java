package com.cht.mapreduce.inputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 *
 * 将10个文件的内容合并为一个文件
 * 自定以FileInputFormat :
 *   key：为map输入的key(则key为文件名称)
 *   value：为map输入的value (文件内容)
 */
public class MergeFileInputFormat extends FileInputFormat<Text, Text> {

    /**
     * 构建recorder实例对象
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        MergeFileRecorderReader reader =new MergeFileRecorderReader();
        reader.initialize(split,context);
        return reader;
    }
}
