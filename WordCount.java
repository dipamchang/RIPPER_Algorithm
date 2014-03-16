package hadoop.shashank.test;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{
	
	static int MAX_RULE = 6;
	static int MDL = 12; // Minimum Description Length
	static int DL = 0; // Description Length
	static int attr_num = 1, total_rules = 0;
	static int rule_no = 0;
	static String attr_value = "z";
	static int p0, p1, n0, n1;  // FOIL's information gain parameters
	static int pr0, nr0, pr1, nr1; // pruning metric parameters
	static Rule[] rule = new Rule[MAX_RULE]; // Rule set
    static String input = "1.txt"; // Training Records
    static String output = "out";
    static double gain, newGain, oldGain, maxGain = 1; // FOIL's information gain
    static int max_attr_num; 
    static String max_attr_value; // conjunct to be added to the rule
    static int pos = 0, neg = 0;  // Accuracy parameters
    static int[] rpos = new int[MAX_RULE];
    static int[] rneg = new int[MAX_RULE]; // Rule Accuracy parameters
    static double prune_old = 0, prune_new = 0, error = 0; // pruning metric
    static FSDataOutputStream out;
    static enum Counter{ P0, P1, N0, N1, PR0, PR1, NR0, NR1, POS, NEG}; // global counters
    FileSystem fs;
    Path path;
    static HashMap<Integer, String[]> map; // Hash Map for attribute values
    
    public int run(String[] args) throws Exception{        

    	map = new HashMap<Integer, String[]>();
    	String s1[] = {"b", "c", "x", "f", "k", "s"};
    	String s2[] = {"f","g", "y", "s"};
    	String s3[] = {"n", "b","c", "g", "r", "p", "u", "e", "w", "y"};
    	String s4[] = {"t", "f"};
    	String s5[] = {"a", "l", "c", "y", "f", "m", "n", "p", "s"};
    	String s6[] = {"a", "d", "f", "n"};
    	String s7[] = {"c", "w", "d"};
    	String s8[] = {"b", "n"};
    	String s9[] = {"k", "n", "b", "h", "g", "r", "o", "p", "u", "e", "w", "y"};
    	String s10[] = {"e", "t"};
    	String s11[] = {"b", "c", "u", "e", "z", "r"};
    	String s12[] = {"f", "y", "k", "s"};
    	String s13[] = {"f", "y", "k", "s"};
    	String s14[] = {"n", "b", "c", "g", "o", "p", "e", "w", "y"};
    	String s15[] = {"n", "b", "c", "g", "o", "p", "e", "w", "y"};
    	String s16[] = {"p", "u"};
    	String s17[] = {"n", "o", "w", "y"};
    	String s18[] = {"n", "o", "t"};
    	String s19[] = {"c", "e", "f", "l", "n", "p", "s", "z"};
    	String s20[] = {"k", "n", "b", "r", "o", "u", "w", "y"};
    	String s21[] = {"a", "c", "n", "s", "v", "y"};
    	String s22[] = {"g", "l", "m", "p", "u", "w", "d"};
    	map.put(0, s1);
    	map.put(1, s2);
    	map.put(2, s3);
    	map.put(3, s4);
    	map.put(4, s5);
    	map.put(5, s6);
    	map.put(6, s7);
    	map.put(7, s8);
    	map.put(8, s9);
    	map.put(9, s10);
    	map.put(10, s11);
    	map.put(11, s12);
    	map.put(12, s13);
    	map.put(13, s14);
    	map.put(14, s15);
    	map.put(15, s16);
    	map.put(16, s17);
    	map.put(17, s18);
    	map.put(18, s19);
    	map.put(19, s20);
    	map.put(20, s21);
    	map.put(21, s22);
    	
    	Configuration conf = new Configuration();
    	fs = FileSystem.get(conf);
    	path = new Path(output);
    	
        for(rule_no= 0;rule_no<MAX_RULE && DL < MDL;rule_no++)
        {
        	rule[rule_no] = new Rule();
        	p0 = p1 = n0 = 0;
        	n1 = 1;
        	rule[rule_no].class_name = "p";
        	
        	maxGain = 1;
        	error = 0;
        	
        	while(maxGain!=0 && rule[rule_no].length < 3)
        	{
        		attr_num = 1;
        		maxGain = 0;
        		max_attr_num = 0;
        		
        		for(int w=0;w<22;w++)
        		{
        			String[] values = map.get(w);
        			for(int k=0;k<values.length;k++)
        			{
        				attr_value = values[k];
        				int flag = 1;
        		    	for(int m=0;m<=rule_no && flag==1;m++)
        				{
        		    		for(int l=0;l<rule[m].length && flag==1;l++)
        		    		{
        		    			if(attr_num == rule[m].num[l] && attr_value.equals(rule[m].val[l]))
        		    				flag = 0;
        		    			else 
        		    				flag = 1;
        		    		}
        				}
        		    	
        		    	if(flag == 1)
        		    	{
        		    		rule_mapred();
        		    		if(maxGain < gain)
        		    		{
        		    			maxGain = gain;
        		    			max_attr_value = attr_value;
        		    			max_attr_num = attr_num;
        		    		}
        		    	}
        		    	
        		    	fs.delete(path, true);
        			}
        			attr_num++;
        		}
        		
        		if(max_attr_num!=0)
        		{
        			rule[rule_no].num[rule[rule_no].length] = max_attr_num;
        			rule[rule_no].val[rule[rule_no].length] = max_attr_value;
        			rule[rule_no].length++;
        			p0 = p1;
        			n0 = n1;
        			DL++;
        			//prune_mapred();
                	//fs.delete(path, true);
                	//error = (double)nr0/(double)(nr0+pr0);
        		}
        	}
        }
       
        Path filenamePath = new Path("Output.txt");
  
        try
        {
        	if(fs.exists(filenamePath))
        		fs.delete(filenamePath, true);
        	
        	out = fs.create(filenamePath);
        } catch (IOException ioe){
        	System.out.println("IO Exception");
        }
        
    	total_rules = rule_no;
        for(int i=0;i<total_rules;i++)
        {
        	out.writeBytes("Rule" + (i+1) + ": ");
        	System.out.print("Rule" + (i+1) + ": ");
        	for(int j=0;j<rule[i].length;j++)
        	{
        		out.writeBytes("Attribute " + rule[i].num[j] + " = " + rule[i].val[j] + ", ");
        		System.out.print("Attribute " + rule[i].num[j] + " = " + rule[i].val[j] + ", ");
        	}
        	out.writeBytes("Class --> " + rule[i].class_name + "\n");
        	System.out.println("Class --> " + rule[i].class_name);
        }
        System.out.println("Default Rule: Class --> e");
        
        int f = 1;
        for(rule_no=0;rule_no<total_rules;rule_no++)
        {
        	prune_old = 0;
        	prune_new = 1;
        	f = 1;
        	while(prune_old < prune_new && f==1)
        	{
        		if(rule[rule_no].length > 0)
        			prune_mapred();
        		if(prune_old < prune_new)
                	rule[rule_no].length--;
        		
        		if(rule[rule_no].length < 2)
        			f = 0;
        		
        		fs.delete(path, true);
        	}
        }
        
        for(int i=0;i<total_rules;i++)
        {
        	out.writeBytes("Pruned Rule" + (i+1) + ": ");
        	System.out.print("Pruned Rule" + (i+1) + ": ");
        	for(int j=0;j<rule[i].length;j++)
        	{
        		out.writeBytes("Attribute " + rule[i].num[j] + " = " + rule[i].val[j] + ", ");
        		System.out.print("Attribute " + rule[i].num[j] + " = " + rule[i].val[j] + ", ");
        	}
        	out.writeBytes("Class --> " + rule[i].class_name + "\n");
        	System.out.println("Class --> " + rule[i].class_name);
        }
        System.out.println("Default Rule: Class --> e");
        
        accuracy_mapred();
        
		
		out.close();
		return 1;
    }
    public void rule_mapred() throws Exception
    {
    	fs.delete(path, true);
    	JobConf job = new JobConf(getConf(), WordCount.class);
        job.setJarByClass(WordCount.class);
        job.setJobName("Rule Based Classifier");
        job.set("attr_num", String.valueOf(attr_num));
        job.set("attr_value", attr_value);
        job.set("rule_no", String.valueOf(rule_no));
        
        for(int i1=0;i1<=rule_no;i1++)
        {
        	job.set("length"+i1, String.valueOf(rule[i1].length));
        	job.set("class_name"+i1, rule[i1].class_name);
        	for(int i2=0;i2<rule[i1].length;i2++)
        	{
        		job.set("rule_num"+i1+i2, String.valueOf(rule[i1].num[i2]));
        		job.set("rule_value"+i1+i2, rule[i1].val[i2]);
        	}
        }
        
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        RunningJob job1 = JobClient.runJob(job);
        Counters c = job1.getCounters();
        p1 = (int)c.getCounter(Counter.P1);
        n1 = (int)c.getCounter(Counter.N1);
        
        if(p0 == 0 && n0 == 0 && rule[rule_no].class_name.equals("p"))
        {
        	n0 = 2761;
        	p0 = 2571;
        }
        else if(p0 == 0 && n0 == 0 && rule[rule_no].class_name.equals("e"))
        {
        	p0 = 2761;
        	n0 = 2571;
        }
        
        if (p1 == 0 | n1 > 0)
			gain = 0; 
        else 
        {
			newGain = log2((double) p1 / (double) (p1 + n1));
			oldGain = log2((double) p0 / (double) (p0 + n0));
			gain = p1 * (newGain - oldGain);
        }
        
        System.out.println(gain);
    }
    
    public void prune_mapred() throws Exception
    {
    	input = "3.txt";
    	fs.delete(path, true);
    	JobConf job = new JobConf(getConf(), WordCount.class);
        job.setJarByClass(WordCount.class);
        job.setJobName("Rule Based Classifier");
        job.set("rule_no", String.valueOf(rule_no));
        
        for(int i1=0;i1<=rule_no;i1++)
        {
        	job.set("length"+i1, String.valueOf(rule[i1].length));
        	job.set("class_name"+i1, rule[i1].class_name);
        	for(int i2=0;i2<rule[i1].length;i2++)
        	{
        		job.set("rule_num"+i1+i2, String.valueOf(rule[i1].num[i2]));
        		job.set("rule_value"+i1+i2, rule[i1].val[i2]);
        	}
        }
        
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(PruneMapper.class);
        job.setReducerClass(PruneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        RunningJob job1 = JobClient.runJob(job);
        Counters c = job1.getCounters();
        pr0 = (int)c.getCounter(Counter.PR0);
        nr0 = (int)c.getCounter(Counter.NR0);
        pr1 = (int)c.getCounter(Counter.PR1);
        nr1 = (int)c.getCounter(Counter.NR1);
        
        prune_old = (double)(pr0 - nr0)/(double)(pr0 + nr0); 
        prune_new = (double)(pr1 - nr1)/(double)(pr1 + nr1); 
        
    }
    
    public void accuracy_mapred() throws Exception
    {
    	input = "2.txt";
    	
    	JobConf job = new JobConf(getConf(), WordCount.class);
        job.setJarByClass(WordCount.class);
        job.setJobName("Rule Based Classifier");
        job.set("MAX_RULE", String.valueOf(total_rules));
        
        for(int i1=0;i1<total_rules;i1++)
        {
        	job.set("length"+i1, String.valueOf(rule[i1].length));
        	job.set("class_name"+i1, rule[i1].class_name);
        	for(int i2=0;i2<rule[i1].length;i2++)
        	{
        		job.set("rule_num"+i1+i2, String.valueOf(rule[i1].num[i2]));
        		job.set("rule_value"+i1+i2, rule[i1].val[i2]);
        	}
        }
     
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(AccuracyMapper.class);
        job.setReducerClass(AccuracyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        JobClient.runJob(job);
    }
    
    static double log2(double num) {
		return (Math.log(num) / Math.log(2));
	}	
    
    public static void main(String args[]) throws Exception {
        System.exit(ToolRunner.run(new WordCount(), args));
    }
}