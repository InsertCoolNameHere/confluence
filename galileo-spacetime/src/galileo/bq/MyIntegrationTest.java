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

public class MyIntegrationTest {

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
		dr.setFsname1("testfs1");
		dr.setFsname2("testfs2");
		
		Coordinates c1 = new Coordinates(39.8117f, -95.3311f);
		Coordinates c2 = new Coordinates(39.75682f, -94.099f);
		Coordinates c3 = new Coordinates(39.0938f, -94.0209f);
		Coordinates c4 = new Coordinates(39.13858f, -95.51101f);
		//Coordinates c5 = new Coordinates(36.78f, -107.64f);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1); cl.add(c2); cl.add(c3); cl.add(c4);
		
		dr.setPolygon(cl);
		dr.setTime("2017-02-xx-xx");
		dr.setLatRelax(0.05f);
		dr.setLongRelax(0.05f);
		dr.setTimeRelaxation(100);
		
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
		args[0] = "phoenix.cs.colostate.edu";
		args[1] = "5634";
		
		
		if (args.length != 2) {
			System.out.println(
					"Usage: MyIntegrationTest [galileo-hostname] [galileo-port-number]");
			System.exit(0);
		} else {
			try {
				GalileoConnector gc = new GalileoConnector("phoenix.cs.colostate.edu", 5634);
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
