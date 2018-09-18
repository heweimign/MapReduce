package com.learning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SelfConnMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {

        String s = value1.toString();
        String[] split = s.split(",");

        // 作为老板
        context.write(new IntWritable(Integer.parseInt(split[0])),new Text("*_"+split[1]));

        // 作为员工
        context.write(new IntWritable(Integer.parseInt(split[3])),new Text(split[1]));
    }
}
