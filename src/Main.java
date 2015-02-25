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

		// Start a "Timer" for Main run time.
		long begin = new Date().getTime();
		
		// Calculate Stock Volatility
		Job calculate = Job.getInstance();
        calculate.setJarByClass(StockVolatility.class);
        
        // Sort Stock
        Job sort = Job.getInstance();
        sort.setJarByClass(StockSort.class);

		System.out.println("\n============ Start - Stock Volatility ============n");
		
		/* 
		 *  Stock Volatility Calculation Job
		 */
        calculate.setMapperClass(StockVolatility.Map.class);
        calculate.setReducerClass(StockVolatility.Reduce.class);
        
        calculate.setOutputKeyClass(Text.class);
        calculate.setOutputValueClass(DoubleWritable.class);

        calculate.setMapOutputKeyClass(Text.class);
        calculate.setMapOutputValueClass(Text.class);

        calculate.setInputFormatClass(TextInputFormat.class);
        calculate.setOutputFormatClass(TextOutputFormat.class);
        
        System.out.println(args[0]);
        System.out.println(args[1]);

        FileInputFormat.addInputPath(calculate, new Path(args[0]));


        FileOutputFormat.setOutputPath(calculate, new Path("/data/temp-data-file"));
        
        /*
         * Stock Sorting for 10 Least, Greatest Volatilities 
         */
        sort.setMapperClass(StockSort.Map.class);
        sort.setReducerClass(StockSort.Reduce.class);

        sort.setOutputKeyClass(Text.class);
        sort.setOutputValueClass(DoubleWritable.class);

        sort.setInputFormatClass(TextInputFormat.class);
        sort.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sort, new Path("/data"));
        FileOutputFormat.setOutputPath(sort, new Path("OMG"));
        

        // Wait for completion
        boolean statusCalculate = calculate.waitForCompletion(true);
        boolean statusSort = sort.waitForCompletion(true);
        if (statusCalculate && statusSort) {
        	long end = new Date().getTime();
        	long timeToComplete = (end - begin) / 1000;
        	System.out.println("Execution Time of Computation: " + timeToComplete + " seconds\n");
        }

        System.out.println("\n============= End - StockVolatility  =============n");		
    }
}
