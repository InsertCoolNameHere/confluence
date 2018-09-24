package galileo.bq;
/* 
 * 
 * All rights reserved.
 * 
 * CSU EDF Project
 * 
 * This program read a csv-formatted file and send each line to the galileo server
 */

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import galileo.comm.SurveyRequest;
import galileo.dataset.Coordinates;
import galileo.util.MyPorter;

public class AccuracyTest {

	// [START processFile]
	/**
	 * read each line from the csv file and send it to galileo server
	 * 
	 * @param pathtothefile
	 *            path to the csv file
	 * @param galileoconnector
	 *            GalileoConnector instance
	 * @throws Exception
	 */
	
	private static void processFile(GalileoConnector gc) throws Exception {
		
		
		String model = "{\"total_layers\":4,\"stds\":[3.65681805853,5.49723347169,26501053.537],\"means\":[38.4563714286,-97.0325,1.42019833465e+12],\"layer_shapes\":[\"(3, 10)\",\"(10, 5)\",\"(5, 10)\",\"(10, 1)\"],\"indv_layers\":[\"[[ 0.1297826   0.04562843 -0.76873394  0.3292985   0.69048505 -0.62677089  -0.09781644  0.68140084 -0.54616637  0.22487269] [-0.01673928  0.80761489  0.39100655 -0.48050895 -0.63736419 -0.4579155   0.19091349 -0.32250112  0.20280065  0.17707338] [-0.39138492 -0.28743048  0.49453939 -0.10158649 -0.33003973 -0.58594941   0.54072928  0.00235761  0.31080867 -0.12351476]]\",\"[[ 0.03398519 -0.01358913  0.34222935 -0.05199189  0.41926003] [-0.14510805 -0.22969876  0.00895174 -0.64938972 -0.60815784] [-0.1036782   0.12807129  0.29456655 -0.09758277 -0.28740995] [-0.0710335   0.50080406 -0.35049407  0.61029738 -0.29122614] [ 0.20887282  0.58993743 -0.25790014  0.5145226   0.03988065] [ 0.22296666  0.42168062  0.59374045 -0.19851399  0.04569556] [ 0.67420688  0.08401729  0.51796134  0.20525562  0.43568582] [ 0.51906492 -0.09433032 -0.22701308 -0.38586627 -0.43146803] [ 0.48515245  0.65046493 -0.37966165 -0.12019875  0.0823656 ] [ 0.52777742  0.40802335  0.65611044  0.36226706  0.513759  ]]\",\"[[  6.69172845e-01   6.54700209e-01  -1.47846836e-01   1.50559479e-09    2.01538551e-01  -4.66614028e-01  -3.19206561e-01  -5.21555358e-01    3.05087726e-01   7.14606303e-01] [  1.46959725e-01   6.24890243e-01   2.14609396e-01  -1.01129992e-01    4.04198517e-02  -3.12303027e-01   4.28264714e-01   2.26224484e-01    6.18377340e-01  -2.62031481e-01] [  1.92687154e-01  -2.65839952e-01   4.36919916e-01   4.27050919e-04    5.97592640e-01  -8.75653379e-02   2.11044487e-01  -4.98454088e-01   -1.19103353e+00   2.01667863e-01] [ -4.81932123e-01  -2.31995692e-01   3.59513976e-01   5.44728466e-03    2.63782443e-01   1.00115757e-01   2.62360275e-02  -4.17657094e-01    4.04450179e-01  -5.22767890e-01] [  3.25582617e-02   1.24067070e-01   1.75093668e-01  -6.10383729e-03    2.22278009e-01  -1.70109371e-01   1.34011165e-02   5.02228099e-01   -9.04612296e-01   7.11018811e-01]]\",\"[[ 0.57373959] [ 0.75466574] [ 0.54184072] [ 0.03733187] [ 0.71599119] [-0.56307616] [ 0.74044355] [-0.04771943] [-0.73462533] [ 0.38767847]]\"],\"bias_shapes\":[10,5,10,1],\"bias_layers\":[\"[ 0.73054647  0.28031566 -0.30291523 -0.20276951 -0.2352665   0.02081088  0.60293741  0.0661332  -0.71232804  0.74433341]\",\"[ 0.06929422  0.03744117  0.7776229  -0.4152143   0.48419148]\",\"[ 0.34179417  0.49623944  0.07467477 -0.5984923   0.42359087  0.27363348  0.70416571  0.14317833 -0.20175166  0.33603671]\",\"[-0.2261457]\"]}";
		
		double latLength = 3d;
		double lonLength = 3d;
		
		double startLat = 33.841d;
		double endLat = 39.58d;
		
		double startLon = -112.11d;
		double endLon = -101.07d;
		
		float latSmall = (float)ThreadLocalRandom.current().nextDouble(startLat, endLat - latLength);
		float latBig = (float)(latSmall + latLength);
		
		float lonSmall = (float)ThreadLocalRandom.current().nextDouble(startLon, endLon - lonLength); 
		float lonBig = (float)(lonSmall + lonLength);
		
		Coordinates c1 = new Coordinates(latBig, lonSmall);
		Coordinates c2 = new Coordinates(latBig, lonBig);
		Coordinates c3 = new Coordinates(latSmall, lonBig);
		Coordinates c4 = new Coordinates(latSmall, lonSmall);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1); cl.add(c2); cl.add(c3); cl.add(c4);
		
		System.out.println();
		
		SurveyRequest sr = new SurveyRequest("noaafs", 100, "sky_ceiling_height", 0.1d, 0.1d, 12*60*60*1000,true, model);
		sr.setPolygon(cl);
		sr.setTime("2015-01-02-10");
		
		System.out.println("QUERY FOR: "+c1+" "+c2+" "+c3+" "+c4+ "2015-01-02-xx");
		
		try {
			gc.survey(sr);
			Thread.sleep(5000);
		} finally {
			gc.disconnect();
		}
	}
	
	// [START Main]
	/**
	 * Based on command line argument, call processFile method to store the data
	 * at galileo server
	 * 
	 * @param args
	 */
	public static void main(String[] args1) {
		String args[] = new String[3];
		args[0] = "lattice-1.cs.colostate.edu";
		args[1] = "5634";
		args[2] = "/s/green/a/tmp/sapmitra/windTrial";
		
		
		try {
			GalileoConnector gc = new GalileoConnector(args[0], Integer.valueOf(args[1]));
			System.out.println(args[0] + "," + Integer.parseInt(args[1]));
			
			processFile(gc);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Data Integration Finished");
		System.exit(0);
	}
	// [END Main]
}
