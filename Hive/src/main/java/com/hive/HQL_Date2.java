package com.shujia;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HQL_Date2 extends UDF {
    public static String evaluate(){
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        return date.format(new Date());
    }
    public static String evaluate(String format){
        SimpleDateFormat date = new SimpleDateFormat(format);
        return date.format(new Date());
    }
}
