import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class SortStocks {

	/***
	 * 
	 * @author joepeacock
	 */
	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Map our volatility's to the key, which is sorted into the reducer, and the stock name as the value.
			String[] columns = value.toString().split("\t");
			context.write(new Text("Stock"), new Text(columns[0] + "," + columns[1]));
		}
	}
	
	/**
	 * 
	 * @author joepeacock
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			TreeMap<Double, String> sortedStocks = new TreeMap<Double, String>();
			
			for (Text val: values) {
				String[] stock = val.toString().split(",");
				sortedStocks.put(Double.parseDouble(stock[1]), stock[0]);
				
				System.out.println("Key: " + stock[0] + " Value: " + stock[1]);
			}
			
			// If we dont have 20 stocks to print, then just print them all. 
			// Otherwise specify the top 10 and bottom 10.
			if (sortedStocks.size() > 19) {

				context.write(new Text("\nLowest 10 Volatility\n"), new DoubleWritable());
                for (int i =0; i<10; i++) {
                	double volatility = sortedStocks.firstEntry().getKey();
                    String stockName = sortedStocks.firstEntry().getValue();

                    context.write(new Text(stockName), new DoubleWritable(volatility));
                        
                    sortedStocks.remove(sortedStocks.firstEntry().getKey());
                }
                
                context.write(new Text("\nHighest 10 Volatility\n"), new DoubleWritable());
                for(int i =0; i<10; i++) {
                	double volatility = sortedStocks.lastEntry().getKey();
                    String stockName = sortedStocks.lastEntry().getValue();

                    context.write(new Text(stockName), new DoubleWritable(volatility));
                        
                    sortedStocks.remove(sortedStocks.lastEntry().getKey());
                }
			} else {
				context.write(new Text("\nAll Volatility's\n"), new DoubleWritable());
                for (int i = 0; i< sortedStocks.size(); i++) {
                	double volatility = sortedStocks.firstEntry().getKey();
                    String stockName = sortedStocks.firstEntry().getValue();

                    context.write(new Text(stockName), new DoubleWritable(volatility));
                        
                    sortedStocks.remove(sortedStocks.firstEntry().getKey());
                }
			}
		}
	}
}
