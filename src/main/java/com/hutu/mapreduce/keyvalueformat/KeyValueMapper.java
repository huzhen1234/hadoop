package com.hutu.mapreduce.keyvalueformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class KeyValueMapper extends Mapper<Text, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    /**
     * @param key 按照,分割之后第一个字符串
     * @param value value为其余字符串
     */
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, one);
    }
}
