import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GroupStocks {
	
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
	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		private Text stockKey = new Text(); 
		private Text stockValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String row = value.toString();
			String[] columns = row.split(",");
			
			// Ignore first row of data with titles, otherwise map it.
			if (!columns[0].equals("Date") && !columns[6].equals("Adj Close")) {
				
				// Parse Date
				String[] splitDate = columns[0].split("-");
				String year = splitDate[0];
				String month = splitDate[1];
				String day = splitDate[2];
				
				// Grab the Adjusted Close
                String closePrice = columns[6]; 		       	
                
                // Now we grab the file name
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                String stockName = fileName.substring(0, fileName.length()-4);
                
                // Set our key and value
				stockKey.set(stockName + "," + month + "-" + year);
				stockValue.set(day + "," + closePrice);
				
				System.out.println(stockKey.toString() + " -> " + stockValue.toString());

                // Finally write it to our combiner 
				context.write(stockKey, stockValue);
			}
		}
	}
	
	/**
	 * The Reducer function that completes two main tasks.
	 * 
	 * 1. Calculate the monthly return per key (stock)
	 * 
	 * The first run through we iterate through each value. The input is already sorted
	 * so as we pass through the dates, we keep track of when the months change. When this happens 
	 * we run a calculation on the first day after the last month change, and the last value iterated over.
	 * 
	 * 2. Calculate the volatility of the stock
	 * 
	 * Run the calculateVolatility() function outlined below, just runs the equation defined
	 * by volatility. It takes a list of monthly returns for an individual stock, and the number
	 * of months that are calculated. 
	 * 
	 * @author joepeacock
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			TreeMap<Integer, Double> dayInput = new TreeMap<Integer, Double>();

			// Now we iterate on our values.
			for (Text val: values) {
				
				// Grab our inputs
				Integer day = Integer.parseInt(val.toString().split(",")[0]);
				Double closePrice = Double.parseDouble(val.toString().split(",")[1]);
				
				dayInput.put(day, closePrice);
			}	
			
			// Calculate our Monthly Return for this month.
			Double monthBeginPrice = dayInput.firstEntry().getValue();
			Double monthEndPrice = dayInput.lastEntry().getValue();

			Double monthlyReturn = (monthEndPrice - monthBeginPrice) / monthBeginPrice;
			DoubleWritable valueOut = new DoubleWritable(monthlyReturn);
			
			// Get our stock name
			String stockName = key.toString().split(",")[0];
			Text keyOut = new Text(stockName);
			
			System.out.println(keyOut.toString() + " -> " + valueOut.toString());

			context.write(keyOut, valueOut);
		}
	}
}