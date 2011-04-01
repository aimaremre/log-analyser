package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class XmlReader {

    public static class XmlMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private static CassandraProxy cassandra = new CassandraProxy("logAnalyser");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String xmlString = value.toString();

            SAXBuilder builder = new SAXBuilder();
            Reader in = new StringReader(xmlString);
            String result = "";
            try {

                Document doc = builder.build(in);
                Element root = doc.getRootElement();

                String from = root.getAttributeValue("from");

                String to = root.getChild("rcpt").getAttributeValue("to");

                String deliveryTime = root.getAttributeValue("del_time");

                result = from + "," + to + "," + deliveryTime;

                String valueForKV = from;

                cassandra.addLog("toBase", to, deliveryTime, valueForKV);

                context.write(NullWritable.get(), new Text(result));
            } catch (JDOMException ex) {
                Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ParseException ex) {
                Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    //
    // public static class IntSumReducer
    // extends Reducer<Text,IntWritable,Text,IntWritable> {
    // private IntWritable result = new IntWritable();
    //
    // public void reduce(Text key, Iterable<IntWritable> values,
    // Context context
    // ) throws IOException, InterruptedException {
    // int sum = 0;
    // for (IntWritable val : values) {
    // sum += val.get();
    // }
    // result.set(sum);
    // context.write(key, result);
    // }
    // }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<success");
        conf.set("xmlinput.end", "</success>");
        conf.set("io.serializations",
                 "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "xml reader");
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setJarByClass(XmlReader.class);
        job.setMapperClass(XmlMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        Path outPath = new Path(otherArgs[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
