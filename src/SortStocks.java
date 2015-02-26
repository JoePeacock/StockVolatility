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
				sortedStocks.put(Double.parseDouble(stock[0]), stock[1]);
			}
			
			context.write(new Text("\nTop 10 Volatility\n"), new DoubleWritable());

			for (int i =0; i<10; i++) {
				double volatility = sortedStocks.firstEntry().getKey();
				String stockName = sortedStocks.firstEntry().getValue();

				context.write(new Text(stockName), new DoubleWritable(volatility));
				
				sortedStocks.remove(sortedStocks.firstEntry());
			}
			
			context.write(new Text("\nBottom 10 Volatility\n"), new DoubleWritable());
			for(int i =0; i<10; i++) {
				double volatility = sortedStocks.lastEntry().getKey();
				String stockName = sortedStocks.lastEntry().getValue();

				context.write(new Text(stockName), new DoubleWritable(volatility));
				
				sortedStocks.remove(sortedStocks.lastEntry());
			}
		}
	}
}
