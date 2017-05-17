package WordCount.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private String word;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	Path pt=new Path("hdfs://cshadoop1/user/mxb151730/negative-words.txt");
    	Path pt1=new Path("hdfs://cshadoop1/user/mxb151730/positive-words.txt");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader negReader=new BufferedReader(new InputStreamReader(fs.open(pt)));
        BufferedReader posReader=new BufferedReader(new InputStreamReader(fs.open(pt1)));
    	String word_new;
    	String word_new1;
    	Text pos=new Text();
    	pos.set("positive");
    	Text neg=new Text();
    	neg.set("negative");
    	
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
       while (itr.hasMoreTokens()) {
    	word=itr.nextToken();  
       
       while((word_new=negReader.readLine())!=null)
        {
       	if(word.equals(word_new))
        		context.write(neg,one);
       	
      
        }
        while((word_new1=posReader.readLine())!=null)
       {
        	if(word.equals(word_new1))
        		context.write(pos,one);
      
        }
      }
       negReader.close();
       posReader.close();
    }
  }

  public static class IntSumReducer
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
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}