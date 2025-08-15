package com.hutu.mapreduce.liuliang;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * 用来快速生成数据
 */
public class TestFillData {
    public static void main(String[] args) throws IOException {
        String[] arr = new String[]{"pk","lisi","wangwu","heifengni","psk"};
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/testdata.data"));
        for (int i = 0; i < 100000; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 5; j++) {
                sb.append(arr[j]).append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            sb.append("\n");
            writer.write(sb.toString());
        }
    }
}
