package com.cht.mapreduce.wordcount.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateBigFile {

    private static String root = System.getProperty("user.dir");
    private static Long fileMaxSize = (long) (1024 * 1024 * 300);

    public static void main(String[] args) {
        String filePath = root + File.separator + "hadoop.txt";
        File testFile = new File(filePath);
        if (!testFile.exists()) {
            try {
                testFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int sum = 0;
        List<String> list = new ArrayList<>(10000);
        while(testFile.length()<=fileMaxSize) {
            String row = createRowData();
            list.add(row);
            sum++;
            writeToFile(testFile, list);

            // 分段写入文件
            if (sum == 10000) {
                writeToFile(testFile, list);
                sum = 0;
                list.clear();
            }
        }
    }

    /**
     * 生成相关数值
     *
     * @param
     */
    public static String createRowData() {
        // 生成手机号
        StringBuilder tel = new StringBuilder(11);
        tel.append(1);
        for (int i = 0; i < 10; i++) {
            // 生成0-9的数
            int num = (int) (Math.random() * 10);
            tel.append(num);
        }

        String upflow = String.valueOf((int) (Math.random() * 1000000));
        String downflow = String.valueOf((int) (Math.random() * 1000000));

        tel.append("\t");
        tel.append(upflow);
        tel.append("\t");
        tel.append(downflow);
        tel.append(System.lineSeparator());
        return tel.toString();

    }

    /**
     * 将内存中的数写入到指定文件
     *
     * @param file
     */
    public static void writeToFile(File file, List<String> list) {

        try (BufferedWriter out=new BufferedWriter(new FileWriter(file,true))) {
            for(String str:list) {
                out.write(str);
            }
        } catch (Exception e) {
            e.getStackTrace();
        }
    }

}
