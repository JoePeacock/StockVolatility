import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StockVolatility {
	
	/***
	 * Map for Stock Volatility 
	 * 
	 * Output: (stock_name => date,adjusted close price)
	 * 
	 * We need to determine the number of months for an individual stock, as well as the
	 * start and end date for a month, and grab the adjusted close price. With this map output 
	 * we can do all 3 things in our reduce easily. 
	 * 
	 * @author joepeacock
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		// Key
		private Text stockName = new Text(); 

		// Values
		private Text date = new Text();
		private Text closePrice = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/* 
			 * Example Row:
			 * 
			 * Index: 	0		    1       2       3       4       5         6
			 * Titles:  Date        Open	High    Low     Close   Volume    Adjusted Close  
			 * Data:    2014-12-24, 112.58, 112.71, 112.01, 112.01, 14479600, 111.57
			 */						
			String row = value.toString();
			String[] columns = row.split(",");
			
			// Ignore first row of data with titles, otherwise map it.
			if (!columns[0].equals("Date") && !columns[6].equals("Adj Close")) {

                date.set(columns[0].split("-")[1]); 		// columns[0] = Date
                closePrice.set(columns[6]); 		       	// columns[6] = Adjusted Close

                Text stockValue = new Text(date.toString() + "," + closePrice.toString()); 
                
                // Now we grab the file name
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                stockName.set(fileName);

                // Finally output
				context.write(stockName, stockValue);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text stockName = new Text();

		private ArrayList<Float> monthlyReturn = new ArrayList<Float>();
		private String previousMonth = "";
		private float numOfMonths = 0;

		private float startPrice = 0;
		private float endPrice = 0;
		
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// Set the Stock Name as the Key
			stockName.set(key);

			for (Text val: values) {
				
				System.out.println(val);
				
				// Parse date & adjusted close
				String[] stockValues = val.toString().split(",");
//				if (stockValues.length < 2) { 
//					continue;
//				}

				String month = stockValues[0];
				String priceInput = stockValues[1];
				
				float closingPrice = Float.parseFloat(priceInput);
				
				// First time around setup.
				if (startPrice == 0 && previousMonth.equals("")) {
					startPrice = closingPrice;
					previousMonth = month;
				}
				
				/*
				 * We check if the month has changed, and that we're not just starting.
				 * If the month changed, increment the number of months we have seen, and run a calculation
				 * for monthly return.
				 * 
				 * closePrice is set to every stock value. The startPrice is only set when the month changes.
				 * When the month does change, we take the last set closePrice to run our calculation, and 
				 * then set the new startPrice.
				 */
				if (!month.equals(previousMonth) && endPrice != 0) {
					numOfMonths += 1;
					monthlyReturn.add((endPrice - startPrice)/startPrice);
					startPrice = closingPrice;
				}
				previousMonth = month;
				endPrice = closingPrice;
			}

			// Add on the last month value
			numOfMonths += 1;
			monthlyReturn.add((endPrice - startPrice)/startPrice);

			/*
			 * Generate the volatility. The equation is as follows:
			 * 
			 * 1. xbar       = sum(xi)/numOfMonth -> sum is over all values from 0 to N in monthlyReturn
			 * 2. xsum       = sum( (xi-xbar)^2 ) from 0 to N in monthlyReturn
			 * 3. volatility = sqrt( (1/numOfMonth-1)*xsum )
			 */
			
			// 1.
			float xiSum = 0;
			for (int i =0; i<monthlyReturn.size(); i++) {
				xiSum += monthlyReturn.get(i);
			}
			float xBar = xiSum/numOfMonths;

			// 2.
			double xSum = 0;
			for (int i=0; i<monthlyReturn.size(); i++) {
				xSum += Math.pow(monthlyReturn.get(i) - xBar, 2);
			}
			
			// 3.
			double root = (1/(numOfMonths-1))*xSum;
			result.set(Double.toString(Math.sqrt(root)));
			
			System.out.println("OUTPUT: " + stockName.toString() + " " + result.toString());
			context.write(stockName, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
        job.setJarByClass(StockVolatility.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
