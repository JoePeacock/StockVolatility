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
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Job job = new Job(conf, "MatrixMul_phase1");
		//Job job2 = new Job(conf, "MatrixMul_phase2");
		
	     Job job = Job.getInstance();
	     job.setJarByClass(StockVolatility.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(StockSort.class);
		 

		System.out.println("\n**********Matrix_Multiplication_Hadoop-> Start**********\n");

		job.setJarByClass(StockVolatility.class);
		job.setMapperClass(StockVolatility.Map.class);
		job.setReducerClass(StockVolatility.Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
//		job.setNumReduceTasks(5);// decide how many output file
		int NOfReducer1 = Integer.valueOf(args[1]);	
		job.setNumReduceTasks(NOfReducer1);
	
//		job.setPartitionerClass(MatrixMul_phase1.CustomPartitioner.class);

		job2.setJarByClass(StockSort.class);
		job2.setMapperClass(StockSort.Map.class);
		job2.setReducerClass(StockSort.Reduce.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
//		job2.setNumReduceTasks(5);
		int NOfReducer2 = Integer.valueOf(args[1]);
		job2.setNumReduceTasks(NOfReducer2);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path("temp-1"));
//		FileInputFormat.addInputPath(job2, new Path("temp-1"));
//		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		
		FileOutputFormat.setOutputPath(job, new Path("Inter_"+args[1]));
		
		FileInputFormat.addInputPath(job2, new Path("Inter_"+args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("Output_"+args[1]));
		
		job.waitForCompletion(true);
//		boolean status = job.waitForCompletion(true);
		boolean status = job2.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
//			System.out.println("\nJob took " + (end - start) + "milliseconds\n");
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Matrix_Multiplication_Hadoop-> End**********\n");		
//		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
