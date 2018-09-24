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

import galileo.comm.SurveyRequest;
import galileo.dataset.Coordinates;

public class SurveyTest {

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
		
		
		Coordinates c1 = new Coordinates(39.58f, -112.11f);
		Coordinates c2 = new Coordinates(39.58f, -101.07f);
		Coordinates c3 = new Coordinates(33.841f, -101.07f);
		Coordinates c4 = new Coordinates(33.841f, -112.11f);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1); cl.add(c2); cl.add(c3); cl.add(c4);
		
		SurveyRequest sr = new SurveyRequest("noaafs", 500, "sky_ceiling_height", 0.1d, 0.1d, 12*60*60*1000,false, null);
		sr.setPolygon(cl);
		sr.setTime("2015-01-02-xx");
		
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
