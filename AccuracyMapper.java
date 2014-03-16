package hadoop.shashank.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AccuracyMapper
        extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int MAX_RULE;
    Rule[] rule = new Rule[6];
    
    public void configure(JobConf job)
    {
    	MAX_RULE = Integer.parseInt(job.get("MAX_RULE"));
    	for(int j1=0;j1<MAX_RULE;j1++)
    	{
    		rule[j1] = new Rule();
        	rule[j1].class_name = "p";
    	}
    	for(int i1=0;i1<MAX_RULE;i1++)
        {
        	rule[i1].length = Integer.valueOf(job.get("length"+i1, "0"));
        	for(int i2=0;i2<rule[i1].length;i2++)
        	{
        		rule[i1].num[i2] = Integer.valueOf(job.get("rule_num"+i1+i2, "1"));
        		rule[i1].val[i2] = job.get("rule_value"+i1+i2, "z");
        	}
        }
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,Reporter reporter)
            throws IOException {
    	
        String line = value.toString();
        String[] words = line.split(",");
        
        int flag = 0, i = 0;
    	
    	for(i=0;i<MAX_RULE && flag==0;i++)
    	{
    		flag = 1;
    		for(int j=0;j<rule[i].length && flag==1;j++)
    		{
    			if(words[rule[i].num[j]].equals(rule[i].val[j]))
    				flag = 1;
    			else 
    				flag = 0;
    		}
    	}
	
    	if(flag == 1)
    	{
    		if(words[0].equals(rule[i-1].class_name))
    		{
    			word.set("rpos"+(i-1));
    			output.collect(word, one);
    			word.set("pos");
    			output.collect(word, one);
    		}
    		else
    		{
    			word.set("rneg"+(i-1));
    			output.collect(word, one);
    			word.set("neg");
    			output.collect(word, one);
    		}
    	}
    	else
    	{
    		if(words[0].equals("e"))
    		{
    			word.set("pos");
    			output.collect(word, one);
    		}
    		else
    		{
    			word.set("neg");
    			output.collect(word, one);
    		}
    	}
    }
}
