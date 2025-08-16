package com.hutu.mapreduce.nlineinputformat;

import com.hutu.mapreduce.TokenizerMapper;
import com.hutu.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // 设置作业的输入输出路径
        String input = "data/nlformat.data";
        String output = "data/nlformat.out";
        FileUtils.deleFile(conf, output);
        // 设置运行类
        job.setJarByClass(MyDriver.class);
        // 设置mapper和reducer
        job.setMapperClass(NLTokenizerMapper.class);
        job.setReducerClass(NLIntSumReducer.class);
        // 设置mapper的输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置reducer的输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 可选
        // job.setCombinerClass(IntSumReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
