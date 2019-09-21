package com.shujia;

import org.apache.hadoop.hive.ql.exec.UDF;

public class To_upper extends UDF {
    public String evaluate(String str){
        String upStr = str.toUpperCase();
        return upStr;
    }
}
