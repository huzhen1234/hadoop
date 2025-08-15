package com.hutu.mapreduce.keyvalueformat;

import com.hutu.mapreduce.TokenizerMapper;
import com.hutu.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KeyValueDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // 设置keyvalue分隔符，源码里面的配置
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPARATOR,",");
        Job job = Job.getInstance(conf, "word count");
        // 设置作业的输入输出路径
        String input = "data/keyvalueformat.data";
        String output = "data/keyvalueformat.out";
        FileUtils.deleFile(conf, output);
        // 设置运行类
        job.setJarByClass(KeyValueDriver.class);
        // 设置mapper和reducer
        job.setMapperClass(KeyValueMapper.class);
        job.setReducerClass(KeyValueReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
