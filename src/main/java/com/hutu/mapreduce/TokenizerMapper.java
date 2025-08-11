package com.hutu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Text, IntWritable 输出
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 使用逗号作为分隔符
        String[] words = value.toString().split(",");
        for (String w : words) {
            word.set(w);  // trim()去除可能的空格
            context.write(word, one);
        }
    }
}

// 结果为 {"ps":1} {"js":1}
