package com.hutu.mapreduce.nlineinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Text, IntWritable 输出
 */

public class NLTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private static final Logger log = Logger.getLogger(NLTokenizerMapper.class);
    private Text word = new Text();

    @Override
    protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        log.info("setup ============");
    }

    /**
     *
     * @param key 偏移量 不是行号
     * @param value 没一行数据的内容
     * 输入的键值对转化为临时的键值对
     * <0, pc,work,nc>  偏移量  某一行的值。 转化为 <pc,1> <work,1> <nc,1>
     */
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
