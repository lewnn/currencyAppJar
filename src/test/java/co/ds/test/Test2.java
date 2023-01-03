package co.ds.test;

import co.ds.Test;

public class Test2 {
    public static void main(String[] args) throws Exception {
        Test test = new Test();
        long start = System.currentTimeMillis();
        test.test();
        long start2 = System.currentTimeMillis();
        test.test();
        long start3 = System.currentTimeMillis();
        System.out.println(start2 - start);
        System.out.println(start3 - start2);
    }
}
