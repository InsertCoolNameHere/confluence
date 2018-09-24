package galileo.bq;
/* 
 * 
 * All rights reserved.
 * 
 * CSU EDF Project
 * 
 * This program read a csv-formatted file and send each line to the galileo server
 */

import galileo.comm.DataIntegrationRequest;
import galileo.dataset.Block;
import galileo.dataset.Coordinates;
import galileo.dataset.SpatialHint;
import galileo.dataset.feature.FeatureType;
import galileo.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

public class MyMultipleIntegrationNOANAMThroughput {

	// [START processFile]
	/**
	 * read each line from the csv file and send it to galileo server
	 * @param date 
	 * @param c42 
	 * @param c32 
	 * @param c22 
	 * @param c12 
	 * 
	 * @param pathtothefile
	 *            path to the csv file
	 * @param galileoconnector
	 *            GalileoConnector instance
	 * @throws Exception
	 */
	public static int numRequests = 1000;
	private static void processFile(GalileoConnector gc, Coordinates c1, Coordinates c2, Coordinates c3, Coordinates c4, int date, int month) throws Exception {
		
		// CREATING FS1
		
		DataIntegrationRequest dr = new DataIntegrationRequest(); 
		dr.setFsname1("namfs");
		dr.setFsname2("noaafs");
		
		/*Coordinates c1 = new Coordinates(34.8117f, -99.3311f);
		Coordinates c2 = new Coordinates(34.78682f, -95.209f);
		Coordinates c3 = new Coordinates(31.45682f, -95.209f);
		Coordinates c4 = new Coordinates(31.62858f, -99.59101f);*/
		
		
		/*Coordinates c1 = new Coordinates(40.96f, -109.11f);
		Coordinates c2 = new Coordinates(40.96f, -102.07f);
		Coordinates c3 = new Coordinates(37.021f, -102.07f);
		Coordinates c4 = new Coordinates(37.021f, -100.11f);
		//Coordinates c5 = new Coordinates(36.78f, -107.64f);*/
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1); cl.add(c2); cl.add(c3); cl.add(c4);
		
		dr.setPolygon(cl);
		
		if(date<10) {
			dr.setTime("2015-0"+month+"-0"+ date +"-xx");
		} else {
			dr.setTime("2015-0"+month+"-"+ date +"-xx");
		}
		
		/*dr.setLatRelax(0.1f);
		dr.setLongRelax(0.1f);
		dr.setTimeRelaxation(1000*60*60);
		dr.setInterpolatingFeature("wind_speed");
*/
		
		//dr.setTime("2015-01-02-xx");
		
		dr.setLatRelax(0.01f);
		dr.setLongRelax(0.01f);
		dr.setTimeRelaxation(1000*60*60*6);
		dr.setInterpolatingFeature("sky_ceiling_height");
		dr.setFixedBeta(true);
		
		
		System.out.println("QUERY FOR: "+c1+" "+c2+" "+c3+" "+c4+" 2015-"+month+"-"+ date +"-xx");
		try {
			gc.integrate(dr);
			Thread.sleep(100);
		} finally {
			//gc.disconnect();
		}
	}
	
	
	public static void individualIntegrationRequests(GalileoConnector gc1) throws Exception {
		
		
		// Country Wide
		float latLength = 11f;
		float longLength = 22f;
		float startLong = -179.0f;
		float endLong = 179.0f;
		
		
		// STATE LEVEL
		//float latLength = 3.8f;
		//float longLength = 7f;
		
		//COUNTY LEVEL
		//float latLength = 0.3f;
		//float longLength = 0.34f;
		
		//CITY LEVEL
		//float latLength = 0.05f;
		//float longLength = 0.1f;
		
		
		float startLat = -90f;
		float endLat = 90f;
		
		//float startLong = -136.0f;
		//float endLong = -62.0f;
		
		
		int count = 0;
		try {
			while(count < numRequests) {
				
				int num = count+1;
				if(count > 90)
					num = ThreadLocalRandom.current().nextInt(1,90);
				System.out.println("REQUESTING LATTICE-"+num);
				GalileoConnector gc = new GalileoConnector("lattice-"+ num +".cs.colostate.edu", 5634);
				
				float lowLat = (float)ThreadLocalRandom.current().nextDouble(startLat, endLat - latLength);
				while(lowLat+latLength > endLat) {
					lowLat = (float)ThreadLocalRandom.current().nextDouble(startLat, endLat - latLength);
				}
				float lowLong = (float)ThreadLocalRandom.current().nextDouble(startLong, endLong - longLength);
				while(lowLong+longLength > endLong) {
					lowLong = (float)ThreadLocalRandom.current().nextDouble(startLong, endLong - longLength);
				}
				int date = ThreadLocalRandom.current().nextInt(1, 31);
				int month = ThreadLocalRandom.current().nextInt(1, 4);
				if(lowLat+latLength < endLat && lowLong+longLength < endLong) {
				
					Coordinates c1 = new Coordinates(lowLat+latLength, lowLong);
					Coordinates c2 = new Coordinates(lowLat+latLength, lowLong+longLength);
					Coordinates c3 = new Coordinates(lowLat, lowLong+longLength);
					Coordinates c4 = new Coordinates(lowLat, lowLong);
					
					processFile(gc, c1,c2,c3,c4, date, month);
					
					
					//Thread.sleep(5);
					//Thread.sleep(10000);
					
					count++;
					System.out.println(count+"==========================================");
					
				}
			}
		} finally {
			gc1.disconnect();
		}
		
		
		
	}
	
	
	public static void main1(String arg[]) {
		for(int i=0; i< 30; i++)
		System.out.println(ThreadLocalRandom.current().nextInt(1990, 11310));
	}
	// [START Main]
	/**
	 * Based on command line argument, call processFile method to store the data
	 * at galileo server
	 * 
	 * @param args
	 */
	public static void main(String[] args1) {
		String args[] = new String[2];
		args[0] = "lattice-1.cs.colostate.edu";
		args[1] = "5634";
		
		
		if (args.length != 2) {
			System.out.println(
					"Usage: MyIntegrationTest [galileo-hostname] [galileo-port-number]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = new GalileoConnector(args[0], Integer.valueOf(args[1]));
				System.out.println(args[0] + "," + Integer.parseInt(args[1]));
				individualIntegrationRequests(gc);
				//processFile(gc);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Data Integration Finished");
		System.exit(0);
	}
	// [END Main]
}
