package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EmailSorter {
	public static class EmailSortMapper 
    extends Mapper<Object, Text, IntWritable,Text>{
 
 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
	 try{
//		 System.out.println(value.toString());
		 String[] columns = value.toString().split("\\x09");
//		 System.out.println(columns.length);
		 String email = columns[0];
		 context.write(new IntWritable(Integer.parseInt(columns[1])), new Text(email));
   }catch(Exception e){
 	  System.err.println(e);
   }
 }
}

public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 if (otherArgs.length != 2) {
   System.err.println("Usage: wordcount <in> <out>");
   System.exit(2);
 }
 Job job = new Job(conf, "email sorter");
 job.setJarByClass(EmailSorter.class);
 job.setMapperClass(EmailSortMapper.class);
 job.setOutputKeyClass(IntWritable.class);
 job.setOutputValueClass(Text.class);
 job.setSortComparatorClass(IntWritableDecreasingComparator.class);
 FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
}
private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
	public int compare(WritableComparable a, WritableComparable b) {
		return -super.compare(a, b);
	}
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return -super.compare(b1, s1, l1, b2, s2, l2);
	}
}
}

