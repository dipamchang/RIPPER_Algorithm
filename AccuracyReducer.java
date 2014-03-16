package hadoop.shashank.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AccuracyReducer 
        extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	int pos = 0, neg = 0;
    int[] rpos = new int[6];
    int[] rneg = new int[6];
	
	public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output,
            Reporter reporter)
            throws IOException {
		
        int sum = 0;
        while (values.hasNext()) {
        	sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
        
        if(key.toString().equals("rpos0"))
	        rpos[0] = sum;
        else if(key.toString().equals("rpos1"))
	        rpos[1] = sum;
        else if(key.toString().equals("rpos2"))
	        rpos[2] = sum;
        else if(key.toString().equals("rpos3"))
	        rpos[3] = sum;
        else if(key.toString().equals("rpos4"))
	        rpos[4] = sum;
        else if(key.toString().equals("rpos5"))
	        rpos[5] = sum;
        else if(key.toString().equals("rpos6"))
	        rpos[6] = sum;
        else if(key.toString().equals("rpos7"))
	        rpos[7] = sum;
        else if(key.toString().equals("rneg0"))
	        rneg[0] = sum;
        else if(key.toString().equals("rneg1"))
	        rneg[1] = sum;
        else if(key.toString().equals("rneg2"))
	        rneg[2] = sum;
        else if(key.toString().equals("rneg3"))
	        rneg[3] = sum;
        else if(key.toString().equals("rpos4"))
	        rneg[4] = sum;
        else if(key.toString().equals("rpos5"))
	        rneg[5] = sum;
        else if(key.toString().equals("rneg6"))
	        rpos[6] = sum;
        else if(key.toString().equals("rneg7"))
	        rpos[7] = sum;
        else if(key.toString().equals("pos"))
	        pos = sum;
        else if(key.toString().equals("neg"))
	        neg = sum;
        
        for(int i=0;i<6;i++)
        {
			if(rpos[i]!=0 | rneg[i]!=0)
			{
				System.out.println("R" + (i+1) + " Accuracy = " + (double)rpos[i]/(double)(rpos[i] + rneg[i])*100 + "% (Coverage = " + (rpos[i] + rneg[i]) + ")");
			}
        }
		if(pos!=0 | neg!=0)
		{
			System.out.println("Total Accuracy = " + (double)pos/(double)(pos + neg)*100 + "%");
		}
    }
}
