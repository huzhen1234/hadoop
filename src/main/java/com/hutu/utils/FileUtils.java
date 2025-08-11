package com.hutu.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileUtils {

    public static void deleFile(Configuration configuration,
                                String path) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        boolean exists = fs.exists(new Path(path));
        if(exists){
            fs.delete(new Path(path), true);
        }
    }
}
