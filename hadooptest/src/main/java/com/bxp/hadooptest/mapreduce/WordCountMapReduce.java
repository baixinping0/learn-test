package com.bxp.hadooptest.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapReduce extends Configured implements Tool{
    /*
    step1 :map class
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text mapOutputKey = new Text();
        private static final IntWritable mapOutputValue = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            //String[] strs = line.split(" ");不使用此方法，由于此方法太消耗内存，而数据量较大
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while(stringTokenizer.hasMoreTokens()){
                String wordValue = stringTokenizer.nextToken();
                //设置值
                mapOutputKey.set(wordValue);
                //输出，此处的输出结果通过shuffle过程后传入reduce
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    //step2 :reduce class
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            //(key values) = (key, list(1, 1, 1))
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    //step3 :driver
    public  int run(String[] arg0) throws Exception{
//        Configuration configuration = new Configuration();
        //读取配置文件信息
        Configuration conf = getConf();
        //创建Job
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置运行的jar
        job.setJarByClass(this.getClass());

        //设置input
        Path inpath = new Path(arg0[0]);
        FileInputFormat.addInputPath(job, inpath);

        //设置map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
        int status = ToolRunner.run(configuration, new WordCountMapReduce(), args);

        System.exit(status);
    }
}