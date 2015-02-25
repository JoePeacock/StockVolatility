import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		// Key
		private Text stockName = new Text(); 

		// Values
		private Text date = new Text();
		private Text closePrice = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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

				
				String[] splitDate = columns[0].split("-");

                date.set(columns[0]); 		// columns[0] = Date
                closePrice.set(columns[6]); 		       	// columns[6] = Adjusted Close

                Text stockValue = new Text(date.toString() + "," + closePrice.toString()); 
                
                // Now we grab the file name
                String nameParse = ((FileSplit) context.getInputSplit()).getPath().getName();
                stockName.set(nameParse);

                // Finally output
				context.write(stockName, stockValue);
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

		/*
         * Generate the volatility. The equation is as follows:
         * 
         *	  x_i         = Invidividual Monthly Return 
         * 1. x_bar       = sum(xi)/numOfMonth -> sum is over all values from 0 to N in monthlyReturn
         * 2. x_sum       = sum( (xi-xbar)^2 ) from 0 to N in monthlyReturn
         * 3. volatility = sqrt( (1/numOfMonth-1)*xsum )
         * 
         */
		private double calculateVolatility(ArrayList<Float> monthlyReturn, float numOfMonths) {
			
			
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
			double volatility = Math.sqrt(root);

			return volatility;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Setup our initial variables
			ArrayList<Float> monthlyReturn = new ArrayList<Float>();
			Text stockName = new Text();

			String previousMonth = "";
			float numOfMonths = 0;

			float startPrice = 0;
			float endPrice = 0;

			// Set the Stock Name as the Key
			stockName.set(key);

			// Now we iterate on our values.
			for (Text val: values) {
				System.out.println(val);
				
				// Parse date & adjusted close value
				String[] stockValues = val.toString().split(",");
				if (stockValues.length < 2) { 
					continue;
				}

				String month = stockValues[0];
				String priceInput = stockValues[1];
				float closingPrice = Float.parseFloat(priceInput);
				
				// First iteration of the loop we set our default values.
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

			// Finally, we calculate our last value.
			numOfMonths += 1;
			monthlyReturn.add((endPrice - startPrice)/startPrice);
			
			// If the volatility is 0 just dont include it.
			double output = calculateVolatility(monthlyReturn, numOfMonths);
			if (output > 0 && !Double.isNaN(output)) {
				context.write(stockName, new DoubleWritable(output));
			}
		}
	}

	
}