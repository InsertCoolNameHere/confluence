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

public class MyIntegrationWindSensor {

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
		
		// CREATING FS1
		
		DataIntegrationRequest dr = new DataIntegrationRequest(); 
		dr.setFsname1("sensorfsnew");
		dr.setFsname2("windfs");
		
		/*Coordinates c1 = new Coordinates(34.8117f, -99.3311f);
		Coordinates c2 = new Coordinates(34.78682f, -95.209f);
		Coordinates c3 = new Coordinates(31.45682f, -95.209f);
		Coordinates c4 = new Coordinates(31.62858f, -99.59101f);*/
		
		
		
		/*Coordinates c1 = new Coordinates(45.22f, -112.9f);
		Coordinates c2 = new Coordinates(45.22f, -100.77f);
		Coordinates c3 = new Coordinates(39.121f, -100.77f);
		Coordinates c4 = new Coordinates(39.121f, -112.9f);*/
		
		Coordinates c1 = new Coordinates(34.22f, -101.9f);
		Coordinates c2 = new Coordinates(34.22f, -89.57f);
		Coordinates c3 = new Coordinates(27.421f, -89.57f);
		Coordinates c4 = new Coordinates(27.421f, -101.9f);
		//Coordinates c5 = new Coordinates(36.78f, -107.64f);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1); cl.add(c2); cl.add(c3); cl.add(c4);
		
		dr.setPolygon(cl);
		dr.setTime("2016-02-05-xx");
		dr.setLatRelax(0.1f);
		dr.setLongRelax(0.1f);
		dr.setTimeRelaxation(1000*60*60);
		dr.setInterpolatingFeature("wind_speed");
		
		try {
			gc.integrate(dr);
			Thread.sleep(10000);
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
		String args[] = new String[2];
		args[0] = "lattice-21.cs.colostate.edu";
		args[1] = "5634";
		
		
		if (args.length != 2) {
			System.out.println(
					"Usage: MyIntegrationTest [galileo-hostname] [galileo-port-number]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = new GalileoConnector(args[0], Integer.valueOf(args[1]));
				System.out.println(args[0] + "," + Integer.parseInt(args[1]));
				
				processFile(gc);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("Data Integration Finished");
		System.exit(0);
	}
	// [END Main]
}
