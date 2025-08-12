package com.hutu.mapreduce.liuliang;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LiuLiangMapper extends Mapper<LongWritable, Text, Text, LiuLiangAccess> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LiuLiangAccess>.Context context) throws IOException, InterruptedException {
        // 使用逗号作为分隔符
        String[] words = value.toString().split(" ");
        LiuLiangAccess access = new LiuLiangAccess(words[1], Long.parseLong(words[2]), Long.parseLong(words[3]));
        context.write(new Text(words[1]), access);
    }
}
