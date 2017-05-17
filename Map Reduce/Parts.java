package WordCount.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Parts {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    HashMap<Integer, Integer> hmap1=new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> hmap2=new HashMap<Integer, Integer>();
	HashMap<Integer, HashMap<String, Integer>> hmap3=new HashMap<Integer, HashMap<String, Integer>>();
    private String word,word_new;
    Text w=new Text();
    public String getParts(char a)
    {
    	if(a=='N'){
    		String k="Noun";
   			return k;
   		}
   		else if(a=='V'||a=='t'||a=='i'){
   			String k="Verb";
   			return k;
   		}
   		else if(a=='A'){
   			String k="Adjective";
   			return k;
   		}
   		else if(a=='v'){
   			String k="Adverb";
   			return k;
   		}
   		else if(a=='C'){
   			String k="Conjunction";
   			return k;
   		}
   		else if(a=='P'){
   			String k="Preposition";
   			return k;
   		}
   		else if(a=='!'){
   			String k="Interjection";
   			return k;
   		}
   		else if(a=='r'){
   			String k="Pronoun";
   			return k;
   		}
   		else{
   			String k="";
   			return k;
   		}
   			
   		
    }
    public Boolean isPal(String word)
    {
    	String rev = new StringBuffer(word).reverse().toString();
        if(word.equals(rev))
        {
     	  
     	   
     	   return true;
        }
        else
        	return false;
    }
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	Path pt=new Path("hdfs://cshadoop1/user/mxb151730/pos.txt");
    	FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader negReader=new BufferedReader(new InputStreamReader(fs.open(pt)));
    	
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
       while (itr.hasMoreTokens()) {
    	word=itr.nextToken(); 
    	if(word.length()>=5)
    	{
    		 while((word_new=negReader.readLine())!=null)
    	        {
    				if(word_new.contains(word))
    		       	{
    		       		Text y=new Text();
    		       		int len=word_new.length();
    		       		y.set(Integer.toString(word.length()));
    		       		//Text x=new Text();
    		       		//Text z=new Text();
    		       		//z.set(word);
    		       		char a=word_new.charAt(len-1);
    		       		String j=getParts(a);
    		       		if (j!="")
    		       		{
    		       			if(hmap1.containsKey(word.length()))
        	    				hmap1.put(word.length(),hmap1.get(word.length())+1);
        	    				else
        	    					hmap1.put(word.length(),1);
        	    				if(isPal(word)){
        	    				if (hmap2.containsKey(word.length()))
        	    				hmap2.put(word.length(),hmap2.get(word.length())+1);
        	    				else
        	    					hmap2.put(word.length(),1);
    		       		}
        	    				HashMap<String, Integer> hmap4=new HashMap<String, Integer>();
        	    				if(hmap3.containsKey(word.length())){
        	    				     hmap4.putAll(hmap3.get(word.length()));
        	    				     if(hmap4.containsKey(j)){
        	    				    	 hmap4.put(j,hmap4.get(j)+1);
        	    				     }
        	    				     hmap3.put(word.length(),hmap4);
        	    				}
        	    				else{
        	    					hmap4.put(j,1);
        	    					hmap3.put(word.length(),hmap4);
        	    				}
        	    				break;
    	        }
    	}
       
    	} }
    }
       for(Entry<Integer, Integer> e1 : hmap1.entrySet()){
       	Integer keyhm1=e1.getKey();
       w.set(keyhm1+ " "+ "count of words");
       one.set(e1.getValue());
       context.write(w,one);
       }
       
       for(Entry<Integer,HashMap<String,Integer>> e3 : hmap3.entrySet()){
       HashMap<String, Integer> temp=new HashMap<String, Integer>();
       Integer keyhm3=e3.getKey();
       temp=hmap3.get(keyhm3);
       for(Entry<String,Integer> e : temp.entrySet() ){
      	 String s1=keyhm3 + " " + e.getKey();
      	 w.set(s1);
      	 one.set(e.getValue());
      	 context.write(w,one);
       }
       }
       
       for(Entry<Integer, Integer> e2 : hmap2.entrySet()){
       	Integer keyhm2=e2.getKey();
       w.set(keyhm2 + " "+ "No. of Palindromes");
       one.set(e2.getValue());
       context.write(w,one);
       }
  }}

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
    job.setJarByClass(Parts.class);
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