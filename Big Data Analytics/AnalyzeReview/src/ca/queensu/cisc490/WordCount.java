package ca.queensu.cisc490;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// To run the program :
//yarn jar AnalyzeReview.jar /reviews.txt /tmp/outputrev2 positive.txt negative.txt
// Order of arguments:
//1 - Path to input file (Review.txt)
//2 - Output path
//3 - Path to positive.txt 
//4 - Path to negative.txt

public class WordCount {
	
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

	  private Text prodID=new Text();
	  private final static IntWritable pos=new IntWritable(1);
	  private final static IntWritable neg=new IntWritable(-1);
	  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  	
    	ArrayList<String> positiveList=new ArrayList<String>(Arrays.asList(context.getConfiguration().getStrings("my-pos-list")));
    	ArrayList<String> negativeList=new ArrayList<String>(Arrays.asList(context.getConfiguration().getStrings("my-neg-list")));
    	
    		String[] current_review=value.toString().split("\\r?\\t");
    		if(current_review.length == 8) {
    			prodID.set(current_review[1]);
    			String review_body=current_review[7];
    			
    			String[] reviewWords=review_body.split(" ");
    			for(String word:reviewWords) {
    				
    				if(positiveList.contains(word.toLowerCase().trim())) {
    					context.write(prodID, pos);
    				}
    				else if(negativeList.contains(word.toLowerCase().trim())) {
    					context.write(prodID, neg);
    				}
    				else{}

    			}
    			
    			
    		}
    	
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
	  
    ArrayList<String> positiveWords=new ArrayList<String>();
	ArrayList<String> negativeWords=new ArrayList<String>();

	try {	  
		String pos_path=args[2];
		String neg_path=args[3];
		File posfile=new File(pos_path);
		File negfile=new File(neg_path);
		
		try {
				BufferedReader posbr=new BufferedReader(new FileReader(posfile));
				BufferedReader negbr=new BufferedReader(new FileReader(negfile));
				String st;
				try {
					while((st=posbr.readLine())!= null) {
						positiveWords.add(st.toLowerCase().trim());
					}
					posbr.close();
					while((st=negbr.readLine())!= null) {
						negativeWords.add(st.toLowerCase().trim());
					}
					negbr.close();
				}
				catch(IOException e) {
						System.out.println(e);
				}
				
			}
			catch(FileNotFoundException fe) {
				System.out.println(fe);
			}
		
		  Configuration conf = new Configuration(); 	
		 
		  String[] PositiveArray=positiveWords.toArray(new String[positiveWords.size()]);
		  String[] NegativeArray=negativeWords.toArray(new String[negativeWords.size()]);
    	  conf.setStrings("my-pos-list",PositiveArray);
    	  conf.setStrings("my-neg-list", NegativeArray);
    	  
    	  
		  Job job = Job.getInstance(conf,"Review Analyzer"); 
		  job.setJarByClass(WordCount.class);
		  job.setMapperClass(TokenizerMapper.class);
		  job.setCombinerClass(IntSumReducer.class);
		  job.setReducerClass(IntSumReducer.class); 
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class); 
		  TextInputFormat.addInputPath(job,new Path(args[0])); 
		  TextOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
		  

	}
	catch(Exception e) {
		System.out.println("Exception Occured :"+e);
	}
	  
  }
}