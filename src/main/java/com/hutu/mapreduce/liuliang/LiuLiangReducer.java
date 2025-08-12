package com.hutu.mapreduce.liuliang;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 如果不需要key，则设置输出的key为NullWritable
 * 此时可以设置 LiuLiangAccess中没有phone属性，如果没有的话，则key就为Text
 */
public class LiuLiangReducer extends Reducer<Text, LiuLiangAccess, NullWritable, LiuLiangAccess> {

    @Override
    protected void reduce(Text key, Iterable<LiuLiangAccess> values, Reducer<Text, LiuLiangAccess, NullWritable, LiuLiangAccess>.Context context) throws IOException, InterruptedException {
        Long downSum = 0L;
        Long upSum = 0L;
        for (LiuLiangAccess access : values) {
            downSum += access.getDown();
            upSum += access.getUp();
        }
        context.write(NullWritable.get(), new LiuLiangAccess(key.toString(), upSum, downSum));
    }
}
