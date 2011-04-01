package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EmailCountWithSendtime {

	
	
	
  public static class TokenizerMapperWithSendtime 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private static final String split = "@";
    private static final String s1 = "[";
    private static final String s2 = "]";
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
//      System.out.println(value.toString());
    	try{
      char delimitor = 94;
      String[] columns = value.toString().split("\\0136");
//      System.out.println(columns.length);
      String sendtime=columns[10];
      String email = columns[3];
    	  String emailType = (email.split(split))[1];
    	  StringBuffer sb = new StringBuffer();
    	  sb.append(sendtime).append(s1).append(emailType).append(s2);
    	  word.set(sb.toString());
    	  context.write(word, one);
      }catch(Exception e){
    	  System.err.println(e);
      }
    }
  }
  
  public static class IntSumReducerForSendtime 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "email counter");
    job.setJarByClass(EmailCountWithSendtime.class);
    job.setMapperClass(TokenizerMapperWithSendtime.class);
    job.setCombinerClass(IntSumReducerForSendtime.class);
    job.setReducerClass(IntSumReducerForSendtime.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}