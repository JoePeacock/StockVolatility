import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class CalculateVolatility {

	public static class Map extends Mapper<Object, Text, Text, DoubleWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] columns = value.toString().split("\t");
			
			Text stockName = new Text(columns[0]);
			DoubleWritable monthlyReturn =  new DoubleWritable(Double.parseDouble(columns[1]));

			context.write(stockName, monthlyReturn);
		}

	}
	
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        /*
         * Generate the volatility. The equation is as follows:
         * 
         *	  x_i         = Invidividual Monthly Return 
         * 1. x_bar       = sum(xi)/numOfMonth -> sum is over all values from 0 to N in monthlyReturn
         * 2. x_sum       = sum( (xi-xbar)^2 ) from 0 to N in monthlyReturn
         * 3. volatility = sqrt( (1/numOfMonth-1)*xsum )
         * 
         */
		private double calculateVolatility(ArrayList<Double> monthlyReturn, float numOfMonths) {
			double xiSum = 0;
            for (int i =0; i<monthlyReturn.size(); i++) {
                xiSum += monthlyReturn.get(i);
            }
            double xBar = xiSum/numOfMonths;

        	double xSum = 0;
        	for (int i=0; i<monthlyReturn.size(); i++) {
                xSum += Math.pow(monthlyReturn.get(i) - xBar, 2);
        	}
        
        	double root = (1/(numOfMonths-1))*xSum;
        	return Math.sqrt(root);
        }

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<Double> returnList = new ArrayList<Double>();

			for (DoubleWritable val: values) {
				returnList.add(val.get());
			}

			int numOfMonths = returnList.size();

			if (numOfMonths > 1) {
				DoubleWritable volatility = new DoubleWritable(calculateVolatility(returnList, numOfMonths));
				Text stockName = key;
				if (volatility.get() !=  0){
					context.write(stockName, volatility);
				}
			}
		}
	}
}
