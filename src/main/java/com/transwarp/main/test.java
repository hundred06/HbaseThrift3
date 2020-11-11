package com.transwarp.main;

public class test {
    public static void main(String[] args) throws Exception {
        String s = "D:\\fuangguan\\明悦\\1D003\\1D003 上海工艺美术研究所\\1D003上海工艺美术研究所历史照片\\1.jpg";
        System.out.println(s.replace("\uFEFF", ""));
//        for(String ss: s.split("\\.")){
//            System.out.println(ss);
//        }
    }
}
