package com.hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class ReadUtil {

    /**
     * 公共读取数据方法
     */
    public static <T> ArrayList<T> read(String path, Class<T> tClass) throws Exception {

        FileReader fileReader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        ArrayList<T> strings = new ArrayList<T>();

        String line;
        while ((line = bufferedReader.readLine()) != null) {

            String[] strs = line.split(",");

            T t = tClass.newInstance();

            /**
             * 给对象里面的所有属性设置值
             */

            //获取了类的所有属性
            Field[] fields = tClass.getDeclaredFields();

            for (int i = 0; i < fields.length; i++) {
                String name = fields[i].getName();//属性名
                Class type = fields[i].getType();//属性的类型
                String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
                //获取方法对象
                Method method = tClass.getMethod(methodName, type);

                /**
                 *s数据里面的字段顺序必须和类里面属性的顺序一样
                 *
                 */
                String str = strs[i];
                if (type.getName().equals("java.lang.String")){
                    method.invoke(t,str);
                }else {
                    Method valueOf = type.getMethod("valueOf", String.class);
                    //多态，父类引用执行之类对象
                    Object value = valueOf.invoke(type, str);
                    method.invoke(t,value);
                }
            }
            strings.add(t);
        }
        bufferedReader.close();
        fileReader.close();
        return strings;
    }
}
