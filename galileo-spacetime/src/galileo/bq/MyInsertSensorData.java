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
import galileo.util.GeoHash;
import galileo.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

public class MyInsertSensorData {

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
		
		//Create a filesystem first.
		
		List<Pair<String, FeatureType>> featureList = new ArrayList<>();
		
  		featureList.add(new Pair<>("platform_id", FeatureType.STRING));
  		featureList.add(new Pair<>("date", FeatureType.STRING));
  		featureList.add(new Pair<>("time", FeatureType.STRING));
  		featureList.add(new Pair<>("datetime", FeatureType.STRING));
		featureList.add(new Pair<>("frac_days_since_jan1", FeatureType.FLOAT));
		featureList.add(new Pair<>("frac_hrs_since_jan1", FeatureType.FLOAT));
		featureList.add(new Pair<>("julian_days", FeatureType.FLOAT));
		featureList.add(new Pair<>("non_epoch_time", FeatureType.FLOAT));
		featureList.add(new Pair<>("alarm_status", FeatureType.FLOAT));
		featureList.add(new Pair<>("inst_status", FeatureType.FLOAT));
		featureList.add(new Pair<>("cavity_pressure", FeatureType.FLOAT));
		featureList.add(new Pair<>("cavity_temp", FeatureType.FLOAT));
		featureList.add(new Pair<>("das_temp", FeatureType.FLOAT));
		featureList.add(new Pair<>("etalon_temp", FeatureType.FLOAT));
		featureList.add(new Pair<>("warm_box_temp", FeatureType.FLOAT));
		featureList.add(new Pair<>("species", FeatureType.FLOAT));
		featureList.add(new Pair<>("mvp_position", FeatureType.FLOAT));
		featureList.add(new Pair<>("outlet_valve", FeatureType.FLOAT));
		featureList.add(new Pair<>("solenoid_valves", FeatureType.FLOAT));
		featureList.add(new Pair<>("co2", FeatureType.FLOAT));
		featureList.add(new Pair<>("co2_dry", FeatureType.FLOAT));
		featureList.add(new Pair<>("ch4", FeatureType.FLOAT));
		featureList.add(new Pair<>("ch4_dry", FeatureType.FLOAT));
		featureList.add(new Pair<>("h2o", FeatureType.FLOAT));
		featureList.add(new Pair<>("gps_abs_lat", FeatureType.FLOAT));
		featureList.add(new Pair<>("gps_abs_lon", FeatureType.FLOAT));
		featureList.add(new Pair<>("gps_fit", FeatureType.FLOAT));
		featureList.add(new Pair<>("gps_time", FeatureType.FLOAT));
		featureList.add(new Pair<>("ws_wind_lon", FeatureType.FLOAT));
		featureList.add(new Pair<>("ws_wind_lat", FeatureType.FLOAT));
		featureList.add(new Pair<>("ws_cos_heading", FeatureType.FLOAT));
		featureList.add(new Pair<>("ws_sin_heading", FeatureType.FLOAT));
		featureList.add(new Pair<>("wind_n", FeatureType.FLOAT));
		featureList.add(new Pair<>("wind_e", FeatureType.FLOAT));
		featureList.add(new Pair<>("wind_dir_sdev", FeatureType.FLOAT));
		featureList.add(new Pair<>("ws_rotation", FeatureType.FLOAT));
		featureList.add(new Pair<>("car_speed", FeatureType.FLOAT));
		featureList.add(new Pair<>("postal_code", FeatureType.STRING));
		featureList.add(new Pair<>("locality", FeatureType.STRING));
		featureList.add(new Pair<>("s2_30_int", FeatureType.LONG));
		featureList.add(new Pair<>("wind_speed", FeatureType.FLOAT));
		featureList.add(new Pair<>("wind_dir", FeatureType.STRING));
		featureList.add(new Pair<>("epoch_time", FeatureType.FLOAT));
		
		
		SpatialHint sp = new SpatialHint("gps_abs_lat", "gps_abs_lon");
		String temporalHint = "epoch_time";
		if(!FS_CREATED){
			gc.createFS("sensorfs", sp, featureList, temporalHint,3);
			//FS_CREATED = true;
		}
		
		FileInputStream inputStream = null;
		Scanner sc = null;
		
		/* START---- THIS PART JUST VERIFIES IF THE DATA FORMAT IS CORRECT */
		try {
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
				String tmpvalues[] = line.split(",");
				if (line.contains("platform_id,date,")) {
					continue;
				}
				if (Float.parseFloat(tmpvalues[24]) == 0.0f && Float.parseFloat(tmpvalues[25]) == 0.0f) {
					continue;
				}
				if (line.contains("NaN") || line.contains("null")) {
					line.replace("NaN", "0.0");
					line.replace("null", "0.0");
				}
				long epoch = GalileoConnector.reformatDatetime(tmpvalues[7]);
				line+=","+epoch;
				c.setTimeInMillis(epoch);
				
				String currentDay = String.format("%d-%d-%d", c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1,
						c.get(Calendar.DAY_OF_MONTH));
				
				String geoHash = GeoHash.encode(Float.parseFloat(tmpvalues[24]), Float.parseFloat(tmpvalues[25]), 4);
				currentDay+="-"+geoHash;
				
				// DOING FOR ONE DAY AT A TIME
				if (previousDay != null && !currentDay.equals(previousDay)) {
					String allLines = data.toString();
					//System.out.println("Creating a block for " + previousDay + " GMT having " + rowCount + " rows");
					//System.out.println(lastLine);
					
					/*Using the lastline to create metadata */
					Block tmp = GalileoConnector.createBlock(lastLine, allLines.substring(0, allLines.length() - 1));
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
				
				if(rowCount % 10000 == 0)
					System.out.println(rowCount);
			}
			/* END---- THIS PART JUST VARIFIES IF THE DATA FORMAT IS CORRECT */
			
			String allLines = data.toString();
			System.out.println("Creating a block for " + previousDay + " GMT having " + rowCount + " rows");
			System.out.println(lastLine);
			Block tmp = GalileoConnector.createBlock(lastLine, allLines.substring(0, allLines.length() - 1));
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
			gc.disconnect();
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
	public static void main(String[] args1) {
		String args[] = new String[3];
		args[0] = "lattice-1.cs.colostate.edu";
		args[1] = "5634";
		args[2] = "/s/lattice-64/a/nobackup/galileo/sapmitra/sensor/part-r-00000";
		
		
		if (args.length != 3) {
			System.out.println(
					"Usage: ConvertCSVFileToGalileo [galileo-hostname] [galileo-port-number] [path-to-csv-file]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = new GalileoConnector(args[0], Integer.valueOf(args[1]));
				System.out.println(args[0] + "," + Integer.parseInt(args[1]));
				File file = new File(args[2]);
				if (file.isFile()) {
					System.out.println("processing - " + args[2]);
					processFile(args[2], gc);
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
	}
	// [END Main]
}
