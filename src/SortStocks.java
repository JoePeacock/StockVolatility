import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class SortStocks {

	/***
	 * 
	 * @author joepeacock
	 */
	public static class Map extends Mapper<Object, Text, DoubleWritable, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Map our volatility's to the key, which is sorted into the reducer, and the stock name as the value.
			String[] columns = value.toString().split("\t");
			context.write(new DoubleWritable(Double.parseDouble(columns[1])), new Text(columns[0]));
		}
	}
	
	/**
	 * 
	 * @author joepeacock
	 *
	 */
	public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		}
	}
}
