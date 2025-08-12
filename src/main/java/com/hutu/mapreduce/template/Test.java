package com.hutu.mapreduce.template;

public class Test {
    public static void main(String[] args) {
        PKMapper mapper = new PKMapperImpl();
        mapper.run();
        
        PKMapper mapper2 = new DPKMapperImpl();
        mapper2.run();
    }
}
