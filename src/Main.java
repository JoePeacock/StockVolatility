import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static void main(String[] args) throws Exception {

		long start = new Date().getTime();		

		Path fileName = new Path("/data/" + args[0].split("/")[1]);
		Path tempFile1 = new Path("/temp/temp1");
		Path tempFile2 = new Path("/temp/temp2");
		
		/*
		 * Setup our Jobs
		 */
	    Job group = Job.getInstance();
	    group.setJarByClass(GroupStocks.class);

	    Job calculate = Job.getInstance();
	    calculate.setJarByClass(CalculateVolatility.class);
	    
	    Job sort = Job.getInstance();
	    calculate.setJarByClass(SortStocks.class);

		System.out.println("\n=============== Stock Volatility - Start ===============\n");
		
		/*
		 * Run GroupStocks Job
		 */
		group.setMapperClass(GroupStocks.Map.class);
        group.setReducerClass(GroupStocks.Reduce.class);
        
        group.setOutputKeyClass(Text.class);
        group.setOutputValueClass(DoubleWritable.class);

        group.setMapOutputKeyClass(Text.class);
        group.setMapOutputValueClass(Text.class);

        group.setInputFormatClass(TextInputFormat.class);
        group.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(group, fileName);
        FileOutputFormat.setOutputPath(group, tempFile1);

		group.waitForCompletion(true);
		
	    /*
		 * Run CalculateVolatility Job
		 */
		calculate.setJarByClass(CalculateVolatility.class);
		calculate.setMapperClass(CalculateVolatility.Map.class);
		calculate.setReducerClass(CalculateVolatility.Reduce.class);

		calculate.setOutputKeyClass(Text.class);
		calculate.setOutputValueClass(DoubleWritable.class);

		calculate.setMapOutputKeyClass(Text.class);
		calculate.setMapOutputValueClass(DoubleWritable.class);

		calculate.setInputFormatClass(TextInputFormat.class);
		calculate.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(calculate, tempFile1);
		FileOutputFormat.setOutputPath(calculate, tempFile2);
		
		calculate.waitForCompletion(true);	
		
		/*
		 * Run Sort Job
		 */
		sort.setMapperClass(GroupStocks.Map.class);
        sort.setReducerClass(GroupStocks.Reduce.class);
        
        sort.setOutputKeyClass(Text.class);
        sort.setOutputValueClass(DoubleWritable.class);

        sort.setMapOutputKeyClass(Text.class);
        sort.setMapOutputValueClass(Text.class);

        sort.setInputFormatClass(TextInputFormat.class);
        sort.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sort, tempFile2);
        FileOutputFormat.setOutputPath(sort, new Path("Output_"+args[1]));

		sort.waitForCompletion(true);

		System.out.println("\n=============== Stock Volatility - End ===============\n");		

		long end = new Date().getTime();
        System.out.println("\nExecution Time: " + (end-start)/1000 + " seconds\n");
	}
}