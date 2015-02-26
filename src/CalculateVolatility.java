import java.util.ArrayList;


public class CalculateVolatility {
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
}
