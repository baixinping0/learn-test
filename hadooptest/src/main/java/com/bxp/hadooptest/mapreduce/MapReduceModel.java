package com.bxp.hadooptest.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReduceModel extends Configured implements Tool{
    
   //TODO  map的输出类型
    public static class ModelMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
           //TODO     具体的map的业务逻辑
            
        }
    }

    //step2 :reduce class
    //TODO    reduce的输入输出类型
    public static class ModelReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            //TODO     具体的reduce的业务逻辑
        }
    }

    //step3 :driver
    public  int run(String[] arg0) throws Exception{
//        Configuration configuration = new Configuration();
        //读取配置文件信息
        Configuration conf = getConf();
//        conf.set("mapreduce.map.output.compress", "true");//设置开启压缩，默认为false
//        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.Lz4Codec");//设置压缩算法,CompressionCodec为压缩算法的父类
//        CompressionCodec

//        conf.set("mapreduce.job.reduces", "2");  //设置reduce的个数
        //创建Job
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置运行的jar
        job.setJarByClass(this.getClass());

        //设置input
        Path inpath = new Path(arg0[0]);
        FileInputFormat.addInputPath(job, inpath);

        //设置map
        //TODO    需要修改ModelMapper
        job.setMapperClass(ModelMapper.class);
        //TODO    需要修改map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//*******************shuffle************************

//        job.setPartitionerClass(cls);     //设置分区
//        job.setSortComparatorClass(cls);    //设置排序
//        job.setCombinerClass(cls);          //设置combiner
//        job.setGroupingComparatorClass(cls);    //设置group

//*******************shuffle************************
        //设置reduce
        //TODO    需要修改ModelReduce
        job.setReducerClass(ModelReduce.class);
        //TODO    需要修改reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setNumReduceTasks(2);   //设置reduce的个数

        //设置output
        Path outpath = new Path(arg0[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        //提交job,设置为true会在运行的时候打印日志信息
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        int status= new WordCountMapReduce().run(args);
        int status = ToolRunner.run(configuration, new MapReduceModel(), args);

        System.exit(status);
    }
}