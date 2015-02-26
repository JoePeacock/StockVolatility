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

	    Job group = Job.getInstance();
	    group.setJarByClass(GroupStocks.class);

	    Job job2 = Job.getInstance();
	    job2.setJarByClass(SortStocks.class);

		System.out.println("\n=============== Stock Volatility - Start ===============\n");

		Path fileName = new Path("/data/" + args[0].split("/")[1]);
		Path tempFile = new Path("temp_file");
		
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
        FileOutputFormat.setOutputPath(group, tempFile);

		group.waitForCompletion(true);

		if (true) {
			long end = new Date().getTime();
			System.out.println("\nExecution Time: " + (end-start)/1000 + " seconds\n");
		}

		System.out.println("\n=============== Stock Volatility - End ===============\n");		

//		FileOutputFormat.setOutputPath(calculate, tempFile);


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
		
	}
}
