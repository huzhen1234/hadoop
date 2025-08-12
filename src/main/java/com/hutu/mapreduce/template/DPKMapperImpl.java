package com.hutu.mapreduce.template;

public class DPKMapperImpl extends PKMapper {
    @Override
    public void setup() {
        System.out.println("--------------DPKMapperImpl setup");
    }

    @Override
    public void map() {
        System.out.println("--------------DPKMapperImpl map");
    }

    @Override
    public void cleanup() {
        System.out.println("--------------DPKMapperImpl cleanup");
    }
}
