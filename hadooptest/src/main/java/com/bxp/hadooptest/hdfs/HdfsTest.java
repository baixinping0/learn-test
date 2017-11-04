package com.bxp.hadooptest.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HdfsTest {

    public static FileSystem getFileSystem(){
        //read core-site.xml,core-default.xml,hdfs-site.xml,hdfs-default.xml
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return fs;
    }

    public static void readFile(String fileName){
        FileSystem fs = getFileSystem();
        Path path = new Path(fileName);
        FSDataInputStream in = null;
        try {
            in = fs.open(path);
            IOUtils.copyBytes(in, System.out, 1024, false);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }

    public static void uploadFile(String srcPath, String destPath){
        FileSystem fs = getFileSystem();
        FileInputStream in = null;
        FSDataOutputStream out = null;
        try {
            in = new FileInputStream(new File(srcPath));
            out = fs.create(new Path(destPath));
            IOUtils.copyBytes(in, out, 1024, false);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

    @Test
    public void testUploadFile(){
        String srcPath = "/usr/lib/hadoop-2.5.0-cdh5.3.6/input/test1";
        String destPath = "/user/bxp/mapreduce/wordcount/input2/test1";
        uploadFile(srcPath, destPath);
    }

    @Test
    public void testReadFile(){
        String fileName = "/user/bxp/mapreduce/wordcount/input/test";
        String fileName2 = "/user/bxp/mapreduce/wordcount/output/part-r-00000";
        readFile(fileName2);
    }

    @Test
    public void printFileSystem(){
        System.out.println(getFileSystem());
    }

}
