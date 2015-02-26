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
	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			context.write(new Text("INPUT"), new Text("FUCKER"));

		}
	}
	
	/**
	 * 
	 * @author joepeacock
	 *
	 */
	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			context.write(new Text("STOCK NAME OUTPUT"), new DoubleWritable(9234.234234));

		}
	}
}
