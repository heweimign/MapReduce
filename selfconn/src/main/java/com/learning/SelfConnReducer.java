package com.learning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SelfConnReducer extends Reducer<IntWritable,Text,Text,Text> {
    @Override
    protected void reduce(IntWritable key3, Iterable<Text> values3, Context context) throws IOException, InterruptedException {

        StringBuilder bossNameStr = new StringBuilder();
        StringBuilder empNameStr = new StringBuilder();

        for(Text text:values3){

            String s = text.toString();
            if(s.startsWith("*_")){
                String bossActualName = s.substring(2);
                bossNameStr.append(bossActualName);
            }else {
                String empName = s;
                empNameStr.append(s).append(",");
            }
        }
        context.write(new Text(bossNameStr.toString()),new Text(empNameStr.toString()));
    }
}
