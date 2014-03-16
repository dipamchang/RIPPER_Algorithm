package hadoop.shashank.test;

import hadoop.shashank.test.WordCount.Counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper 
        extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    int attr_num;
    String attr_value;
    int rule_no;
    
    Rule[] rule = new Rule[6];
    
    public void configure(JobConf job)
    {
    	attr_value = job.get("attr_value");
    	attr_num = Integer.parseInt(job.get("attr_num", "1"));
    	rule_no = Integer.parseInt(job.get("rule_no"));
    	
    	for(int j1=0;j1<6;j1++)
    	{
    		rule[j1] = new Rule();
        	rule[j1].class_name = "p";
    	}
    	for(int i1=0;i1<=rule_no && i1<6;i1++)
        {
        	rule[i1].length = Integer.valueOf(job.get("length"+i1, "0"));
        	for(int i2=0;i2<rule[i1].length;i2++)
        	{
        		rule[i1].num[i2] = Integer.valueOf(job.get("rule_num"+i1+i2, "1"));
        		rule[i1].val[i2] = job.get("rule_value"+i1+i2, "z");
        	}
        }
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
        String line = value.toString();
        String[] words = line.split(",");        

        if(compare(words) == 1)
        {
        	if(words[0].equals(rule[rule_no].class_name) && words[attr_num].equals(attr_value))
        	{
        		reporter.incrCounter(Counter.P1, 1);
        		word.set("p1");
        		output.collect(word, one);
        	}
        	else if(words[attr_num].equals(attr_value))
        	{
        		reporter.incrCounter(Counter.N1, 1);
        		word.set("n1");
        		output.collect(word, one);
        	}
        }
    }
    
    public int compare(String[] word)
    {
    	int flag1 = 0, flag2 = 1, i = 0;
    	
    	for(i=0;i<rule_no && flag1==0;i++)
    	{
    		flag1 = 1;
    		for(int j=0;j<rule[i].length && flag1==1;j++)
    		{
    			if(word[rule[i].num[j]].equals(rule[i].val[j]))
    				flag1 = 1;
    			else 
    				flag1 = 0;
    		}
    	}
    	
    	for(int j=0;j<rule[rule_no].length && flag2==1;j++)
		{
			if(word[rule[rule_no].num[j]].equals(rule[rule_no].val[j]))
				flag2 = 1;
			else 
				flag2 = 0;
		}
    	
    	if(flag1 == 0 && flag2 == 1)
    		return 1;
    	else 
    		return 0;
    }
}