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

public class MyInsertDummySensorData {

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
	private static boolean FS_CREATED = false;
	
	private static void processFile(File[] files, GalileoConnector gc) throws Exception {
		
		// CREATING FS1
		if( ! FS_CREATED ) {
			List<Pair<String, FeatureType>> featureList1 = new ArrayList<>();
	  		
			featureList1.add(new Pair<>("epoch_time", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lat", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lon", FeatureType.FLOAT));
			featureList1.add(new Pair<>("cavity_pressure", FeatureType.FLOAT));
			featureList1.add(new Pair<>("cavity_temp", FeatureType.FLOAT));
			featureList1.add(new Pair<>("ch4", FeatureType.FLOAT));
			
			
			SpatialHint sp1 = new SpatialHint("gps_abs_lat", "gps_abs_lon");
			String temporalHint1 = "epoch_time";
			//if(!FS_CREATED){
			gc.createFS("sensorfsdummyfinal", sp1, featureList1, temporalHint1, 1);
			FS_CREATED = true;
			Thread.sleep(1000);
			
		}
		try {
			insertData(files, gc, "sensorfsdummyfinal", 1);
			Thread.sleep(1000);
		} finally {
			
		}
	}
	/**
	 * @param filepath
	 * @param gc
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 * @throws Exception
	 * @throws IOException
	 */
	private static void insertData(File[] files, GalileoConnector gc, String fsName, int mode)
			throws FileNotFoundException, UnsupportedEncodingException, Exception, IOException {
		FileInputStream inputStream = null;
		Scanner sc = null;
		
		try{
			int count = 0;
			System.out.println(files.length);
			for(File f : files) {
				count++;
				if(count%1000 == 0)
					System.out.println("\n\n============="+count+"============\n\n");
				//System.out.println("processing - " + f);
				String filepath = f.getAbsolutePath();
				inputStream = new FileInputStream(filepath);
				sc = new Scanner(inputStream);
				StringBuffer data = new StringBuffer();
				//System.out.println("Start Reading CSV File");
				String currentDay = null;
				int rowCount = 0;
				Calendar c = Calendar.getInstance();
				c.setTimeZone(TimeZone.getTimeZone("GMT"));
				String firstLine = "";
				
				while (sc.hasNextLine()) {
					String line = sc.nextLine();
					
					if (line.contains("NaN") || line.contains("null")) {
						continue;
					}
					if(line.trim().isEmpty())
						continue;
					String tmpvalues[] = line.split(",");
					if (line.contains("epoch_time") || tmpvalues.length != 6) {
						continue;
					}
					if (Float.parseFloat(tmpvalues[1]) == 0.0f && Float.parseFloat(tmpvalues[2]) == 0.0f) {
						continue;
					}
					
					data.append(line + "\n");
					
					if(firstLine.length() == 0) {
						firstLine = line;
						long epoch = GalileoConnector.reformatDatetime(tmpvalues[0]);
						c.setTimeInMillis(epoch);
						currentDay = String.format("%d-%d-%d", c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
					}
					
					rowCount++;
				}
				
				String allLines = data.toString();
				//System.out.println("Creating a block for " + currentDay + " GMT having " + rowCount + " rows");
				//System.out.println(firstLine);
				
				if(allLines.trim().isEmpty()) {
					continue;
				}
				
				Block tmp = GalileoConnector.createBlockSensorDummy(firstLine, allLines.substring(0, allLines.length() - 1), fsName);
				if (tmp != null) {
					gc.store(tmp);
					Thread.sleep(5);
				}
				inputStream.close();
			}
			
			// note that Scanner suppresses exceptions
			if (sc.ioException() != null) {
				throw sc.ioException();
			}
			} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (sc != null) {
				sc.close();
			}
			
		}
		
	}
	// [END processFile]

	// [START Main]
	/**
	 * Based on command line argument, call processFile method to store the data
	 * at galileo server
	 * 
	 * @param args
	 */
	/*public static void main1(String[] args1) {
		String args[] = new String[3];
		args[0] = "phoenix.cs.colostate.edu";
		args[1] = "5634";
		args[2] = "/s/chopin/b/grad/sapmitra/Documents/Conflux/fs1.csv";
		
		System.out.println(args.length);
		if (args.length != 3) {
			System.out.println(
					"Usage: ConvertCSVFileToGalileo [galileo-hostname] [galileo-port-number] [path-to-csv-file]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = new GalileoConnector("phoenix.cs.colostate.edu", 5634);
				System.out.println(args[0] + "," + Integer.parseInt(args[1]));
				File file = new File(args[2]);
				if (file.isFile()) {
					System.out.println("processing - " + args[2]);
					//processFile(args[2], gc);
				} else {
					if (file.isDirectory()) {
						File[] files = file.listFiles();
						for (File f : files) {
							if (f.isFile())
								System.out.println("processing - " + f);
							processFile(f.getAbsolutePath(), gc);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Data successfully inserted into galileo");
		System.exit(0);
	}*/
	
	public static void main(String[] args1) {
		String args[] = new String[3];
		args[0] = "lattice-1.cs.colostate.edu";
		args[1] = "5634";
		args[2] = "/s/green/a/tmp/sapmitra/LargeSensorData";
		
		if (args.length != 3) {
			System.out.println(
					"Usage: WindInsert [galileo-hostname] [galileo-port-number] [path-to-csv-file]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = null;
				try {
					gc = new GalileoConnector(args[0], Integer.valueOf(args[1]));
					System.out.println(args[0] + "," + Integer.parseInt(args[1]));
					
					String[] dirs = {"Day1-6","Day7-12","Day13-18","Day19-24","Day25-31"};
					
					for(String dirName: dirs) {
						System.out.println("processing - " + args[2]+"/"+dirName);
						File file = new File(args[2]+"/"+dirName);
							if (file.isFile()) {
								
								//processFile(args[2], gc);
							} else {
								if (file.isDirectory()) {
									File[] files = file.listFiles();
									
									processFile(files, gc);
									
									
								}
							}
						}
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
