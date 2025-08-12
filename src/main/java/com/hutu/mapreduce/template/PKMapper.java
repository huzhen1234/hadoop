package com.hutu.mapreduce.template;

public abstract class PKMapper {

    /**
     * 前置 处理
     */
    public abstract void setup();

    /**
     * 业务 处理
     */
    public abstract void map();

    /**
     * 后置 处理
     */
    public abstract void cleanup();

    public void run() {
        setup();
        map();
        cleanup();
    }
}
