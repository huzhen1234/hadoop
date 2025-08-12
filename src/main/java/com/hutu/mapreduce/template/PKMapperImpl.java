package com.hutu.mapreduce.template;

public class PKMapperImpl extends PKMapper {
    @Override
    public void setup() {
        System.out.println("--------------PKMapperImpl setup");
    }

    @Override
    public void map() {
        System.out.println("--------------PKMapperImpl map");
    }

    @Override
    public void cleanup() {
        System.out.println("--------------PKMapperImpl cleanup");
    }
}
