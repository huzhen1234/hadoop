package com.hutu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.Asserts;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.54:8020"), configuration, "huzhen");
        boolean mkdirs = fs.mkdirs(new Path("/huzhen/test"));
        Asserts.check(mkdirs, "创建目录失败");
        // 关闭资源
        if (fs != null){
            fs.close();
        }
    }
}
