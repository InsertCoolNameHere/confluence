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

public class MyInsertTest1 {

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
	
	private static void processFile(String filepath, GalileoConnector gc) throws Exception {
		
		// CREATING FS1
		if( ! FS_CREATED ) {
			List<Pair<String, FeatureType>> featureList1 = new ArrayList<>();
	  		
			featureList1.add(new Pair<>("epoch_time", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lat", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lon", FeatureType.FLOAT));
			featureList1.add(new Pair<>("fsa_feature", FeatureType.FLOAT));
			
			
			SpatialHint sp1 = new SpatialHint("gps_abs_lat", "gps_abs_lon");
			String temporalHint1 = "epoch_time";
			//if(!FS_CREATED){
			gc.createFS("testfs1", sp1, featureList1, temporalHint1, 1);
			FS_CREATED = true;
			//}
			
		}
		try {
			insertData(filepath, gc, "testfs1", 1);
			Thread.sleep(5000);
		} finally {
			gc.disconnect();
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
	private static void insertData(String filepath, GalileoConnector gc, String fsName, int mode)
			throws FileNotFoundException, UnsupportedEncodingException, Exception, IOException {
		FileInputStream inputStream = null;
		Scanner sc = null;
		
		try{
			inputStream = new FileInputStream(filepath);
			sc = new Scanner(inputStream);
			StringBuffer data = new StringBuffer();
			System.out.println("Start Reading CSV File");
			String previousDay = null;
			int rowCount = 0;
			Calendar c = Calendar.getInstance();
			c.setTimeZone(TimeZone.getTimeZone("GMT"));
			String lastLine = "";
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				if(line.trim().isEmpty())
					continue;
				String tmpvalues[] = line.split(",");
				if (line.contains("epoch_time")) {
					continue;
				}
				if (Float.parseFloat(tmpvalues[1]) == 0.0f && Float.parseFloat(tmpvalues[2]) == 0.0f) {
					continue;
				}
				if (line.contains("NaN") || line.contains("null")) {
					line.replace("NaN", "0.0");
					line.replace("null", "0.0");
				}
				long epoch = GalileoConnector.reformatDatetime(tmpvalues[0]);
				c.setTimeInMillis(epoch);
				String currentDay = String.format("%d-%d-%d", c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1,
						c.get(Calendar.DAY_OF_MONTH));
				
				// DOING FOR ONE DAY AT A TIME
				if (previousDay != null && !currentDay.equals(previousDay)) {
					String allLines = data.toString();
					System.out.println("Creating a block for " + previousDay + " GMT having " + rowCount + " rows");
					System.out.println(lastLine);
					
					/*Using the lastline to create metadata */
					Block tmp = GalileoConnector.createBlock(lastLine, allLines.substring(0, allLines.length() - 1), fsName, mode);
					if (tmp != null) {
						gc.store(tmp);
					}
					data = new StringBuffer();
					rowCount = 0;
				}
				previousDay = currentDay;
				data.append(line + "\n");
				lastLine = line;
				rowCount++;
			}
			
			String allLines = data.toString();
			System.out.println("Creating a block for " + previousDay + " GMT having " + rowCount + " rows");
			System.out.println(lastLine);
			Block tmp = GalileoConnector.createBlock1(lastLine, allLines.substring(0, allLines.length() - 1));
			if (tmp != null) {
				gc.store(tmp);
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
	public static void main1(String[] args1) {
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
					processFile(args[2], gc);
				} /*else {
					if (file.isDirectory()) {
						File[] files = file.listFiles();
						for (File f : files) {
							if (f.isFile())
								System.out.println("processing - " + f);
							processFile(f.getAbsolutePath(), gc);
						}
					}
				}*/
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Data successfully inserted into galileo");
		System.exit(0);
	}
	
	
	public static void main(String arg[]) {
		
		String filepath = "sftp://lattice-1/s/lattice-1/a/nobackup/galileo/sapmitra/nam/2015/2015-01-02-8g";
		System.out.println(filepath.substring(filepath.length() - 2, filepath.length() - 1));
		
		String g = "abcd";
		
		System.out.println(g.substring(0, g.length() - 1));
		//Getting date
		String[] tokens = filepath.split("/");
		String fileName = tokens[tokens.length - 1];
		
		String dateString = fileName.substring(0, fileName.length() - 3);
		
		System.out.println(dateString);
		
		String ghash = fileName.substring(fileName.length() - 2, fileName.length());
		System.out.println(ghash);
	}
	// [END Main]
}
