package galileo.bq;
/* 
 * Copyright (c) 2015, Colorado State University. Written by Duck Keun Yang 2015-08-02
 * 
 * All rights reserved.
 * 
 * CSU EDF Project
 * 
 * This program read a csv-formatted file and send each line to the galileo server
 */

import galileo.dataset.Block;
import galileo.dataset.Coordinates;
import galileo.dataset.SpatialHint;
import galileo.dataset.SpatialRange;
import galileo.dataset.feature.FeatureType;
import galileo.util.GeoHash;
import galileo.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

public class MyInsertDummyTeraSensorData {

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
	
	public static int pointsPerFile = 6000;
	
	private static boolean FS_CREATED = false;
	
	private int fileCount = 0;
	public static String fsName = "sensorfsdummytest1";
	
	private static void processFile(GalileoConnector gc, List<String> keys) throws Exception {
		
		System.out.println("FS CREATED?? "+FS_CREATED);
		// CREATING FS1
		if( ! FS_CREATED ) {
			List<Pair<String, FeatureType>> featureList1 = new ArrayList<>();
	  		
			featureList1.add(new Pair<>("epoch_time", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lat", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lon", FeatureType.FLOAT));
			featureList1.add(new Pair<>("cavity_pressure", FeatureType.FLOAT));
			featureList1.add(new Pair<>("cavity_temp", FeatureType.FLOAT));
			featureList1.add(new Pair<>("ch4", FeatureType.FLOAT));
			
			featureList1.add(new Pair<>("xtravar1", FeatureType.FLOAT));
			featureList1.add(new Pair<>("xtravar2", FeatureType.FLOAT));
			featureList1.add(new Pair<>("xtravar3", FeatureType.FLOAT));
			featureList1.add(new Pair<>("xtravar4", FeatureType.FLOAT));
			featureList1.add(new Pair<>("xtravar5", FeatureType.FLOAT));
			
			
			SpatialHint sp1 = new SpatialHint("gps_abs_lat", "gps_abs_lon");
			String temporalHint1 = "epoch_time";
			//if(!FS_CREATED){
			gc.createFSTB(fsName, sp1, featureList1, temporalHint1, 1);
			FS_CREATED = true;
			Thread.sleep(3000);
			
		}
		try {
			insertData(keys, gc, fsName, 1);
			Thread.sleep(1000);
		} finally {
			
		}
	}
	/**
	 * @param filepath
	 * @param gc
	 * @throws FileNotFoundException
	 * @throws Exception
	 */
	private static void insertData(List<String> keys, GalileoConnector gc, String fsName, int mode) throws Exception {
		
		System.out.println("Total Files being Inserted: " + keys.size());
		//Random rn = new Random();
		
		int count = 0;
		
		long filesize = 0;
		
		for(String key : keys) {
			
			
			count++;
			
			if(count %100 == 0) {
				
				System.out.println("\n\n============="+count+"============\n\n");
				System.out.println(filesize);
			}
			
			boolean include = ThreadLocalRandom.current().nextBoolean();
			if(!include) {
				include = ThreadLocalRandom.current().nextBoolean();
				if(!include)
					continue;
				
			}
			
			// Generate x points per plot: 100=small, 1000=medium, 10000=big
			String[] tokens  = key.split("-");
			String year = tokens[0];
			String month = tokens[1];
			String day = tokens[2];
			String geohash = tokens[3];
			
			Calendar c = Calendar.getInstance();
			c.setTimeZone(TimeZone.getTimeZone("GMT"));
			c.set(Calendar.DAY_OF_MONTH, Integer.valueOf(day));
			c.set(Calendar.YEAR, Integer.valueOf(year));
			c.set(Calendar.MONTH, Integer.valueOf(month) - 1);
			
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			
			long baseTime = c.getTimeInMillis();
			
			long adder = 1000*60*60*24;
			
			int numRecs = ThreadLocalRandom.current().nextInt(1000, pointsPerFile);
			
			String allRecs = "";
			String firstLine = "";
			
			for (int i = 0; i < numRecs; i++) {
				
				long timeAdd = ThreadLocalRandom.current().nextLong(0, adder);
				long time = baseTime + timeAdd;
				
				double[] randCoord = randomPoint(geohash);
				
				
				String record = time + ",";
				record += randCoord[0] + "," + randCoord[1] + ",";
				
				//cavity_pressure
				/*record += ThreadLocalRandom.current().nextDouble(80, 85) + ","; // add a random temperature from 80-85 degrees
					//cavity_temp
					record += ThreadLocalRandom.current().nextDouble(.55, .75) + ","; // add a random humidity from 55%-75%
					//ch4
					record += ThreadLocalRandom.current().nextDouble(380, 420) + ","; // add a random CO2 from 380ppm-420ppm
					
					double val = ThreadLocalRandom.current().nextDouble(10, 20) ;
					//xtravar1
					record += (val+25)+ ","; // random variable
					//xtravar2
					record += (val+20)+ ","; // random variable
					//xtravar3
					record += (val+15)+ ","; // random variable
					//xtravar4
					record += (val+10)+ ","; // random variable
					//xtravar5
					record += (val+5); // random variable
*/					
				
				
				
				
				
				//cavity_pressure
				record += 80 + (double)(Math.random() * ((85 - 80) + 1))+","; // add a random temperature from 80-85 degrees
				//cavity_temp
				record += .55 + (double)(Math.random() * ((.75 - .55) + 1)) + ","; // add a random humidity from 55%-75%
				//ch4
				record += 380 + (double)(Math.random() * ((420 - 380) + 1)) + ","; // add a random CO2 from 380ppm-420ppm
				
				double val = 20 + (double)(Math.random() * ((20 - 10) + 1));
				//xtravar1
				record += (val+25)+ ","; // random variable
				//xtravar2
				record += (val+20)+ ","; // random variable
				//xtravar3
				record += (val+15)+ ","; // random variable
				//xtravar4
				record += (val+10)+ ","; // random variable
				//xtravar5
				record += (val+5); // random variable
				
				
				if(i==0)
					firstLine = record;
				
				if(i != numRecs-1)
					allRecs+=record + "\n";
				else
					allRecs+=record;
				
			}
			
			/*if(allRecs.trim().isEmpty()) {
				continue;
			}*/
			
			filesize+=allRecs.getBytes().length;
			
			Block tmp = GalileoConnector.createBlockSensorDummyTB(firstLine, allRecs, fsName);
			if (tmp != null) {
				gc.store(tmp);
				Thread.sleep(1);
			}
			
			
		}
		
		// Megabytes
		filesize = filesize/(1024*1024);
		
		System.out.println("TOTAL FILESIZE: "+filesize+" MB");
			// note that Scanner suppresses exceptions
			
		
		
	}
	// [END processFile]

	
	public static double[] randomPoint(String geoHash) {
		// System.out.println("minX: " + poly.getBounds2D().getMinX() + " maxX: " +
		// poly.getBounds2D().getMaxX() + " minY: "+poly.getBounds2D().getMinY()+ "
		// maxY: " + poly.getBounds2D().getMaxY());
		SpatialRange decodeHash = GeoHash.decodeHash(geoHash);
		
		double minX = decodeHash.getLowerBoundForLatitude();
		double maxX = decodeHash.getUpperBoundForLatitude();
		double minY = decodeHash.getLowerBoundForLongitude();
		double maxY = decodeHash.getUpperBoundForLongitude();

		
		double xCoord = ThreadLocalRandom.current().nextDouble(minX, maxX);
		double yCoord = ThreadLocalRandom.current().nextDouble(minY, maxY);
		
		return new double[] { xCoord, yCoord };
	}
	
	
	
	private static String[] generateGeohashes(String[] geohashes_2char, int spatialHashType) {
		List<String> allGeoHashes = new ArrayList<String>(Arrays.asList(geohashes_2char));
		
		for(int i = 2; i < spatialHashType; i++) {
			
			List<String> currentGeohashes = new ArrayList<String>();
			
			for(String geoHash : allGeoHashes) {
				
				
				SpatialRange range1 = GeoHash.decodeHash(geoHash);
				
				Coordinates c1 = new Coordinates(range1.getLowerBoundForLatitude(), range1.getLowerBoundForLongitude());
				Coordinates c2 = new Coordinates(range1.getUpperBoundForLatitude(), range1.getLowerBoundForLongitude());
				Coordinates c3 = new Coordinates(range1.getUpperBoundForLatitude(), range1.getUpperBoundForLongitude());
				Coordinates c4 = new Coordinates(range1.getLowerBoundForLatitude(), range1.getUpperBoundForLongitude());
				
				ArrayList<Coordinates> cs1 = new ArrayList<Coordinates>();
				cs1.add(c1);cs1.add(c2);cs1.add(c3);cs1.add(c4);
				
				currentGeohashes.addAll(Arrays.asList(GeoHash.getIntersectingGeohashesForConvexBoundingPolygon(cs1, i+1)));
				
			}
			allGeoHashes = currentGeohashes;
			
		}
		Collections.sort(allGeoHashes);
		String[] returnArray = allGeoHashes.toArray(new String[allGeoHashes.size()]);
		return returnArray;
	}
	
	
	public static void generateKeys(List<String> keys, String dateStr, int start) {
		
		
		String[] geohashes = {"9w", "9x", "9y", "9z"};
		String[] allGeoHashes = generateGeohashes(geohashes, 4);
		
		for(int i=start; i <= start+9; i++) {
			for(String g : allGeoHashes) {
				String date = dateStr+i;
				if(i<10) {
					date = dateStr+"0"+i;
				}
				String key = date+"-"+g;
				keys.add(key);
				
			}
		}
	}
	
	
	public static void main1(String[] args1) throws IOException {
	
		
		/*String h = "hello world";
		System.out.println(h.getBytes().length);
		
		
	    BufferedWriter writer = new BufferedWriter(new FileWriter("/s/green/a/tmp/sapmitra/test/me.txt", true));
	    
	    writer.append(h);
	     
	    writer.close();*/
		
		String[] gs = {"9r","9x","9z","dp","dr","9q","9w", "9y", "dn", "dq" };
		
		Arrays.sort(gs);
		for(int i=0; i<gs.length; i++)
			System.out.print("\""+gs[i]+"\",");
	}
	
	public static void main(String[] args) {
		
		/*int start = Integer.valueOf(args[0]);
		String args0 = args[1];
		FS_CREATED = Boolean.valueOf(args[2]);
		String args1 = "5634";
		String args2 = "/s/green/a/tmp/sapmitra/LargeSensorData";*/
		
		int start = 1;
		String args0 = "lattice-1.cs.colostate.edu";
		FS_CREATED = true;
		String args1 = "5634";
		String args2 = "/s/green/a/tmp/sapmitra/LargeSensorData";
		
		
		if (args.length != 0) {
			System.out.println(
					"Usage: WindInsert [galileo-hostname] [galileo-port-number] [path-to-csv-file]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = null;
				try {
					gc = new GalileoConnector(args0, Integer.valueOf(args1));
					System.out.println(args0 + "," + Integer.parseInt(args1));
					
					List<String> keys = new ArrayList<String>();;
					//generateKeys(keys,"2016-05-", start);
					keys.add("2016-05-03-9w00");		
					processFile(gc, keys);
									
									
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					gc.disconnect();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Data successfully inserted into galileo");
		System.exit(0);
	}
	// [END Main]
}
