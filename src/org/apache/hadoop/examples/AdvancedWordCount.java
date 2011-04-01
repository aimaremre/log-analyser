package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 改进的 WordCount 例子
 * @author zhengxq
 *
 */
public class AdvancedWordCount {
    /**
     * 通过扩展 Reducer 实现内部实现类 IntSumReducer
     */
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String pattern="[^\\w]"; //正则表达式,代表不是 0-9, a-z, A-Z 的所有其它字符
        /**
         * 重载了默认的 map 方法,利用 StringTokenizer 将每行字符串拆成单词,
         * 然后将输出结果<单词,1>写入到 OutputCollector 中。
         * OutputCollector 由 Hadoop 框架提供,负责收集 Mapper 和 Reducer 的输出数
据,
         * 实现 map 函数时,只需要简单地将其输出的<key,value> 写入
OutputCollector 即可,其他的处理框架会搞定。
         *
         * 输入参数:
         * value:是文本文件中的一行
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase(); //全部转为小写字母
            System.out.println("----------------line todo:" + line);
            line = line.replaceAll(pattern, " "); //将非 0-9, a-z, A-Z 的字符替换为空格,再分隔
            System.out.println("----------------line done:" + line);
            StringTokenizer itr = new StringTokenizer(line.toString());//默认以空格作为分隔符
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());// word is the KEY
                context.write(word, one); // 把处理中间结果<key,value>写入,默认每个词出现一次
            }
         }
    }
    /**
      * 通过扩展 Reducer 实现内部实现类 IntSumReducer
      */
    public static class IntSumReducer extends
              Reducer<Text, IntWritable, Text, IntWritable> {
         private IntWritable result = new IntWritable();
         /**
           * 重载了默认的 reduce 方法
           * 通过 key 遍历其 values<Iterator>, 并加以处理
           * 所谓的处理就是将相同 key 下的词频相加,就可以得到这个单词的总的出现次
数。
           *
           * 输入参数:
           * key:是任务的(中间)输出结果的 KEY,这里表示一个单词
           * values:是相同 key 下的数值 Iterator,这里是词频
           */
         public void reduce(Text key, Iterable<IntWritable> values,
                  Context context) throws IOException, InterruptedException {
              int sum = 0;
              for (IntWritable val : values) {
                  sum += val.get();//一个一个的加起来
              }
              result.set(sum);
              context.write(key, result);//重新写回去
         }
    }
    public static class MyInverseMapper
         extends Mapper<Object, Text, IntWritable, Text> {
         public void map(Object key, Text value, Context context)
                  throws IOException, InterruptedException {
              //每行只可能是<TEXT VALUE>对
              String keyAndValue[] = value.toString().split("\t");
              System.out.println("-------------------->" + value);
              System.out.println("----------0--------->" + keyAndValue[0]);
              System.out.println("----------1--------->" + keyAndValue[1]);
              //context.write(value, key);
              context.write(new IntWritable(Integer.parseInt(keyAndValue[1])), new
Text(keyAndValue[0]));
         }
    }
    /**
      * 代码内容基本抄自原有 main
      * @param conf
      * @param in
      * @param out
      * @throws IOException
      * @throws ClassNotFoundException
      * @throws InterruptedException
      */
    public static boolean countingJob(Configuration conf, Path in, Path out)
throws IOException, InterruptedException, ClassNotFoundException{
         Job job = new Job(conf, "wordcount");//定义一个 Job
         job.setJarByClass(AdvancedWordCount.class);//设定执行类
         job.setMapperClass(TokenizerMapper.class);//设定 Mapper 实现类,这里为本例实现的 TokenizerMapper.class
         job.setCombinerClass(IntSumReducer.class);//设定 Cominer 实现类,这里为本例实现的 IntSumReducer.class
         job.setReducerClass(IntSumReducer.class);//设定 Reducer 实现类,这里为本例实现的 IntSumReducer.class
         job.setOutputKeyClass(Text.class);//设定 OutputKey 实现类,Text.class 其实是默认的实现,可以不设置
         job.setOutputValueClass(IntWritable.class);//设定 OutputValue 实现类,这里为本例实现的 IntWritable.class
         FileInputFormat.addInputPath(job, in);//设定 job 的输入文件夹,注意这里可以是本地实际存在的文件,或者 hdfs 绝对路径
         FileOutputFormat.setOutputPath(job, out);//设定 job 的输出文件夹,规则同上
         //执行
         return job.waitForCompletion(true);
    }
    /**
      * 这个类有些变化,需要注意
      * @param conf
      * @param in
      * @param out
      * @throws IOException
      * @throws ClassNotFoundException
      * @throws InterruptedException
      */
    public static boolean sorttingJob(Configuration conf, Path in, Path out)
throws IOException, InterruptedException, ClassNotFoundException{
         Job job = new Job(conf, "sort");
         job.setJarByClass(AdvancedWordCount.class);
          job.setMapperClass(MyInverseMapper.class);//这里用了自己重新实现的MyInverseMapper 类
          job.setOutputKeyClass(IntWritable.class);//跟原来反了,这里的 KEY 是词频
          job.setOutputValueClass(Text.class);//跟原来反了,这里的 value 是单词
          job.setSortComparatorClass(IntWritableDecreasingComparator.class);//需要设置排序实现类
          FileInputFormat.addInputPath(job, in);//设定 job 的输入文件夹,注意这里可以是本地实际存在的文件,或者 hdfs 绝对路径
          FileOutputFormat.setOutputPath(job, out);//设定 job 的输出文件夹,规则同上
          //执行
          return job.waitForCompletion(true);
    }
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
             public int compare(WritableComparable a, WritableComparable b) {
               return -super.compare(a, b);
             }
             public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                 return -super.compare(b1, s1, l1, b2, s2, l2);
             }
        }
    /**
      *
      * @param args
      * @throws Exception
      */
    public static void main(String[] args) throws Exception {
          Configuration conf = new Configuration();//启用默认配置
          String[] otherArgs = new GenericOptionsParser(conf,
args).getRemainingArgs();
          Path temp = new Path("wordcount-temp-" +
                  Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
          boolean a=false,b=false;
          Path in = new Path(otherArgs[0]);
          Path out = new Path(otherArgs[1]);
    if (otherArgs.length != 2) {
        System.err.println("Usage: wordcount <in> <out>");
        System.exit(2);
    }
    try{
        a = AdvancedWordCount.countingJob(conf, in, temp);//执行 countingJob
        b = AdvancedWordCount.sorttingJob(conf, temp, out);//执行 sorttingJob
    }catch(Exception e){
        e.printStackTrace();
    }finally{
        FileSystem.get(conf).delete(temp,true);//删除临时目录
        if(!a || !b) FileSystem.get(conf).delete(out, true);//如果不完全成功,删除输出目录,便于下一次运行
    }
  }
}
