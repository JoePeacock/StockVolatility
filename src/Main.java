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

	     Job calculate= Job.getInstance();
	     calculate.setJarByClass(StockVolatility.class);

	     Job job2 = Job.getInstance();
	     job2.setJarByClass(StockSort.class);

		System.out.println("\n=============== Stock Volatility - Start ===============\n");

		Path fileName = new Path("/data/" + args[0].split("/")[1]);
		Path tempFile = new Path("temp_file");
		
		/*
		 * Run StockVolatility Class Job
		 */
		calculate.setMapperClass(StockVolatility.Map.class);
        calculate.setReducerClass(StockVolatility.Reduce.class);
        
        calculate.setOutputKeyClass(Text.class);
        calculate.setOutputValueClass(DoubleWritable.class);

        calculate.setMapOutputKeyClass(Text.class);
        calculate.setMapOutputValueClass(Text.class);

        calculate.setInputFormatClass(TextInputFormat.class);
        calculate.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(calculate, fileName);
        FileOutputFormat.setOutputPath(calculate, new Path("Output_"+args[1]));
//		FileOutputFormat.setOutputPath(calculate, tempFile);

		calculate.waitForCompletion(true);

//		/*
//		 * Run StockSort Class Job
//		 */
//		job2.setJarByClass(StockSort.class);
//		job2.setMapperClass(StockSort.Map.class);
//		job2.setReducerClass(StockSort.Reduce.class);
//
//		job2.setOutputKeyClass(Text.class);
//		job2.setOutputValueClass(DoubleWritable.class);
//
//		job2.setMapOutputKeyClass(Text.class);
//		job2.setMapOutputValueClass(Text.class);
//
//		job2.setInputFormatClass(TextInputFormat.class);
//		job2.setOutputFormatClass(TextOutputFormat.class);
//
//		FileInputFormat.addInputPath(job2, tempFile);
//		FileOutputFormat.setOutputPath(job2, new Path("Output_"+args[1]));
//		
//		boolean status = job2.waitForCompletion(true);
//		
//		/*
//		 * Calculate the Execution Time of the program when job2 finishes.
//		 */
		if (true) {
			long end = new Date().getTime();
			System.out.println("\nExecution Time: " + (end-start)/1000 + " seconds\n");
		}

		System.out.println("\n=============== Stock Volatility - End ===============\n");		
	}
}
