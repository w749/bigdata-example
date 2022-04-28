package org.mlamp.mapreduce;

public class Test {
    @org.junit.Test
    public void worCountTest() {
        String[] args = {"/tmp/wordcount/input/wordcount", "/tmp/wordcount/output/1"};
        try {
            WordCountTest.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
