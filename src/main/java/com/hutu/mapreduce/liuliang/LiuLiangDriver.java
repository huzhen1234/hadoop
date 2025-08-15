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

        //通过这样就把读取进来了，运转机制是什么呢？
        // 将输入文件切分为逻辑上的 InputSplit 每个 InputSplit 分配给一个独立的 Mapper
        // 提供 RecordReader 实现 RecordReader 负责从逻辑 InputSplit 中读取输入记录供 Mapper 处理。 InputSplit交给MapperTask
        // 一个InputSplit 一个Mapper
        // 一个MR作业，Mapper阶段的并行度是由InputSplit 的数量来决定的 默认是按照 32MB大小来切割文件的

        // 在HDFS中是按照块的(block)，在mapreduce中是按照 InputSplit(逻辑上) 来分的
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // TextInputFormat 是 FileInputFormat的实现类，是一行一行的读的，重写了父类的createRecordReader 和 isSplitable 方法
    // KeyValueTextInputFormat 是 FileInputFormat的实现类，键值对，重写了父类的createRecordReader 和 isSplitable 方法
}
