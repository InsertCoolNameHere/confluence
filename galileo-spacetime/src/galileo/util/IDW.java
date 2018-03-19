package galileo.util;

import java.util.List;

/**
 * @author sapmitra
 *
 */
public class IDW {
	
	/**
	 * 
	 * @param aRec all standardized
	 * @param bRecs
	 * @param betas
	 * @param timeSpan 
	 * @param timeMin 
	 * @param lonSpan 
	 * @param lonMin 
	 * @param latSpan 
	 * @param latMin 
	 * @return
	 */
	
	public static String getOneTrainingPoint(String[] aRec, List<String[]> bRecs, double[] betas,
			double latMin, double latSpan, double lonMin, double lonSpan, double timeMin, double timeSpan) {
		
		double[] bDists = new double[bRecs.size()];
		
		double aLatO = Double.valueOf(aRec[0]);
		double aLongO = Double.valueOf(aRec[1]);
		double aTimeO = Double.valueOf(aRec[2]);
		
		double aLat = (Double.valueOf(aRec[0]) - latMin)/latSpan;
		double aLong = (Double.valueOf(aRec[1]) - lonMin)/lonSpan;
		double aTime = (Double.valueOf(aRec[2])- timeMin)/timeSpan;
		
		double actualParameterVal = Double.valueOf(aRec[3]);
		
		double bestBeta = 0;
		double bestError = 99999999999d;
		
		// Calculating Euclidean distances
		int count = 0;
		for(String[] bRec : bRecs) {
			double bLat = (Double.valueOf(bRec[0]) - latMin)/latSpan;
			double bLong = (Double.valueOf(bRec[1]) - lonMin)/lonSpan;
			double bTime = (Double.valueOf(bRec[2])- timeMin)/timeSpan;
			
			double euclideanDist = java.lang.Math.pow((aLat-bLat), 2)
					+java.lang.Math.pow((aLong-bLong), 2) + java.lang.Math.pow((aTime-bTime), 2);
			
			bDists[count] = euclideanDist;
			count++;
		}
		
		// Generating training Points
		for(double beta : betas) {
			// For each beta
			double prediction = 0;
			int cnt = 0;
			double sumWeights = 0;
			
			for(String[] bRec : bRecs) {
				
				double dist = bDists[cnt];
				double bParameterVal = Double.valueOf(bRec[3]);
				// Inverse distance
				double weight = 1 / java.lang.Math.pow(dist, beta);;
				sumWeights += weight;
				prediction += bParameterVal*weight;
			}
			
			prediction = prediction/sumWeights;
			double error = java.lang.Math.abs(actualParameterVal - prediction) / actualParameterVal;
			if(error < bestError) {
				bestError = error;
				bestBeta = beta;
			}
			
		}
			
		String trainingPt = aLatO+","+aLongO+","+aTimeO+","+bestBeta+","+bestError;
		
		return trainingPt;
		
	}
	
	public static double getContribution(double a, double b, double beta) {
		
		
		return 0;
	}

}
