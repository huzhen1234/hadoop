package com.hutu.mapreduce.liuliang;

import com.hutu.mapreduce.IntSumReducer;
import com.hutu.mapreduce.MyDriver;
import com.hutu.mapreduce.TokenizerMapper;
import com.hutu.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LiuLiangDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "liuliang fenxi");

        // 设置作业的输入输出路径
        String input = "data/liuliang.data";
        String output = "data/liuliang.out";
        FileUtils.deleFile(conf, output);
        // 设置运行类
        job.setJarByClass(LiuLiangDriver.class);
        // 设置mapper和reducer
        job.setMapperClass(LiuLiangMapper.class);
        job.setReducerClass(LiuLiangReducer.class);
        // 设置mapper的输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LiuLiangAccess.class);
        // 设置reducer的输出数据的key和value类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LiuLiangAccess.class);
        // 可选
        // job.setCombinerClass(IntSumReducer.class);


        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
