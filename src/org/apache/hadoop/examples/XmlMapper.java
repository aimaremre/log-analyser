package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
      
    public void map(String key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	System.out.println("key-->"+key);
    	System.out.println("value-->"+value);
    	context.write(new Text(key), one);
    }
  }
