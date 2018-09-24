package galileo.util;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

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
	private static final Logger logger = Logger.getLogger("galileo");
	
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
			
			euclideanDist = java.lang.Math.pow(euclideanDist, 0.5);
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
				cnt++;
			}
			
			prediction = prediction/sumWeights;
			double error = java.lang.Math.abs(actualParameterVal - prediction) / actualParameterVal;
			if(error < bestError) {
				bestError = error;
				bestBeta = beta;
			}
			
		}
			
		String trainingPt = aLatO+","+aLongO+","+aTimeO+","+bestBeta+","+bestError+","+actualParameterVal;
		
		return trainingPt;
		
	}
	
	
	public static String getOneComparison(String[] aRec, List<String[]> bRecs, double[] betas,
			double latMin, double latSpan, double lonMin, double lonSpan, double timeMin, double timeSpan) {
		
		double[] bDists = new double[bRecs.size()];
		
		double aLat = (Double.valueOf(aRec[0]) - latMin)/latSpan;
		double aLong = (Double.valueOf(aRec[1]) - lonMin)/lonSpan;
		double aTime = (Double.valueOf(aRec[2])- timeMin)/timeSpan;
		
		double actualParameterVal = Double.valueOf(aRec[3]);
		
		// Calculating Euclidean distances
		int count = 0;
		for(String[] bRec : bRecs) {
			double bLat = (Double.valueOf(bRec[0]) - latMin)/latSpan;
			double bLong = (Double.valueOf(bRec[1]) - lonMin)/lonSpan;
			double bTime = (Double.valueOf(bRec[2])- timeMin)/timeSpan;
			
			double euclideanDist = java.lang.Math.pow((aLat-bLat), 2)
					+java.lang.Math.pow((aLong-bLong), 2) + java.lang.Math.pow((aTime-bTime), 2);
			
			euclideanDist = java.lang.Math.pow(euclideanDist, 0.5);
			
			bDists[count] = euclideanDist;
			count++;
		}
		
		// Generating training Points
		String recordString = "";
		
		
		//logger.info("RIKI: BETAS "+betas[0]+" "+betas[1]+" "+betas[2]+" "+betas[3]+" "+betas[4]+" ");
		
		int c = 0;
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
				cnt++;
			}
			
			prediction = prediction/sumWeights;
			
			//double error = java.lang.Math.abs(actualParameterVal - prediction) / actualParameterVal;
			
			double error = actualParameterVal - prediction;
			error = error*error;
			
			if(c == betas.length - 1)
				recordString+=error;
			else
				recordString+=error + ",";
			
			c++;
		}
			
		String trainingPt = recordString;
		
		return trainingPt;
		
	}
	
	public static String neighborsStringRepresentation(List<String[]> bRecs) {
		String rep = "$$$";
		for(String[] brec : bRecs) {
			rep+=Arrays.toString(brec)+"$$";
		}
		
		return rep;
		
	}
	
	public static String calculateIDW(String[] aRec, List<String[]> bRecs, double[] betas, int interpolatingFeature, int[] aPosns, int[] bPosns) {
		
		double[] bDists = new double[bRecs.size()];
		
		double aLat = Double.valueOf(aRec[aPosns[0]]);
		double aLong = Double.valueOf(aRec[aPosns[1]]);
		double aTime = Double.valueOf(aRec[aPosns[2]]);
		
		// Calculating Euclidean distances
		int count = 0;
		for(String[] bRec : bRecs) {
			double bLat = Double.valueOf(bRec[bPosns[0]]);
			double bLong = Double.valueOf(bRec[bPosns[1]]);
			double bTime = Double.valueOf(bRec[bPosns[2]]);
			
			double euclideanDist = java.lang.Math.pow((aLat-bLat), 2)
					+java.lang.Math.pow((aLong-bLong), 2) + java.lang.Math.pow((aTime-bTime), 2);
			
			bDists[count] = euclideanDist;
			count++;
		}
		
		double prediction = 0;
		for(double beta : betas) {
			// For each beta
			
			int cnt = 0;
			double sumWeights = 0;
			
			for(String[] bRec : bRecs) {
				
				double dist = bDists[cnt];
				double bParameterVal = Double.valueOf(bRec[interpolatingFeature]);
				// Inverse distance
				double weight = 1 / java.lang.Math.pow(dist, beta);;
				sumWeights += weight;
				prediction += bParameterVal*weight;
			}
			
			prediction = prediction/sumWeights;
		}
		
		String bString="";
		for(String[] brec: bRecs) {
			bString+=Arrays.asList(brec)+"**";
		}
		String record = Arrays.asList(aRec)+"<SEP>"+bString+"<PRED>"+prediction;
		
		return record;
		
	}
	
	public static double getContribution(double a, double b, double beta) {
		
		
		return 0;
	}
	
	public static void main(String arg[]) {
		
		System.out.println(Runtime.getRuntime().availableProcessors());
	}

}
