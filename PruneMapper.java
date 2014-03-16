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

public class PruneMapper
        extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    int rule_no;
    Rule[] rule = new Rule[6];
    
    public void configure(JobConf job)
    {
    	rule_no = Integer.parseInt(job.get("rule_no"));
    	for(int j1=0;j1<6;j1++)
    	{
    		rule[j1] = new Rule();
    		rule[j1].class_name = "p";
    	}
    	for(int i1=0;i1<=rule_no &&i1<6;i1++)
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
        int flag = 1;
        
        for(int j=0;j<rule[rule_no].length-1 && flag==1;j++)
		{
			if(rule[rule_no].val[j].equals(words[rule[rule_no].num[j]]))
				flag = 1;
			else 
				flag = 0;
		}
        
        if(flag == 1)
        {
        	if(words[0].equals(rule[rule_no].class_name))
        	{
        		if(rule[rule_no].val[rule[rule_no].length-1].equals(words[rule[rule_no].num[rule[rule_no].length-1]]))
        		{
        			reporter.incrCounter(Counter.PR0, 1);
        			word.set("p0");
        			output.collect(word, one);
        		}
        		reporter.incrCounter(Counter.PR1, 1);
        		word.set("p1");
    			output.collect(word, one);
        	}
        	else
        	{
        		if(rule[rule_no].val[rule[rule_no].length-1].equals(words[rule[rule_no].num[rule[rule_no].length-1]]))
        		{
        			reporter.incrCounter(Counter.NR0, 1);
        			word.set("n0");
        			output.collect(word, one);
        		}
        		reporter.incrCounter(Counter.NR1, 1);
        		word.set("n1");
    			output.collect(word, one);
        	}
        }
    }
}