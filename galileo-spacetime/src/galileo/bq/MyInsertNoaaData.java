package galileo.bq;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;

import galileo.dataset.Block;
import galileo.dataset.SpatialHint;
import galileo.dataset.feature.FeatureType;
import galileo.util.GeoHash;
import galileo.util.Pair;


public class MyInsertNoaaData {
	
	private static String fsName = "noaafs";
	private static boolean FS_CREATED = true;
	public static String baseDir = "/s/lattice-64/a/nobackup/galileo/sapmitra/noaa/ftp.ncdc.noaa.gov/pub/data/noaa/2015";
	//public static String baseDir = "/s/green/a/tmp/sapmitra/noaa_test/2015_test";
	//public static String outputDir = "/s/lattice-64/a/nobackup/galileo/sapmitra/noaa/ftp.ncdc.noaa.gov/pub/data/noaa/2015_classified/";
	//public static String outputDir = "/s/green/a/tmp/sapmitra/noaa_test/2015_classified/";
	//public static List<String> keys = new ArrayList<String>();
	
	public static void main(String arg[]) throws Exception {
		
		//List<String> ln = new ArrayList<String>();
		//ln.add("0083010840999992015010100004+69450+030033FM-12+002799999V0203101N001019999999N999999999-00201-00371999999ADDAA106000091OD139900101999REMSYN04601084 16/// /3101 11020 21037 60001 333 91101=");
		
		insertFiles(baseDir);
		//readLines(ln);
		
	}
	
	
	
	
	private static void insertFiles(String dirName) throws Exception {
		
		GalileoConnector gc = new GalileoConnector("lattice-1.cs.colostate.edu", 5634);
		
		if( ! FS_CREATED ) {
			
			List<Pair<String, FeatureType>> featureList1 = new ArrayList<>();
	  		
			featureList1.add(new Pair<>("gps_abs_lat", FeatureType.FLOAT));
			featureList1.add(new Pair<>("gps_abs_lon", FeatureType.FLOAT));
			featureList1.add(new Pair<>("report_type", FeatureType.STRING));
			featureList1.add(new Pair<>("elevation", FeatureType.FLOAT));
			featureList1.add(new Pair<>("epoch_time", FeatureType.FLOAT));
			featureList1.add(new Pair<>("weather_station_id", FeatureType.STRING));
			
			featureList1.add(new Pair<>("wind_dir_angle", FeatureType.FLOAT));
			featureList1.add(new Pair<>("wind_dir_typecode", FeatureType.STRING));
			featureList1.add(new Pair<>("wind_speed", FeatureType.FLOAT));
			featureList1.add(new Pair<>("sky_ceiling_height", FeatureType.FLOAT));
			featureList1.add(new Pair<>("sky_ceiling_code", FeatureType.STRING));
			featureList1.add(new Pair<>("sky_cavoc", FeatureType.STRING));
			featureList1.add(new Pair<>("visibility_dist", FeatureType.FLOAT));
			featureList1.add(new Pair<>("air_temp", FeatureType.FLOAT));
			featureList1.add(new Pair<>("air_dew_point", FeatureType.FLOAT));
			featureList1.add(new Pair<>("atm_pressure", FeatureType.FLOAT));
			
			
			
			SpatialHint sp1 = new SpatialHint("gps_abs_lat", "gps_abs_lon");
			String temporalHint1 = "epoch_time";
			//if(!FS_CREATED){
			gc.createFSNoaa(fsName , sp1, featureList1, temporalHint1, 1);
			FS_CREATED = true;
			Thread.sleep(1000);
			
		}
		System.out.println("FS CREATION COMPLETE");
		File file = new File(dirName);
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			
			processFile(files, gc);
			
			
		}
		
		
	}




	private static void processFile(File[] files, GalileoConnector gc) throws Exception {
		
		FileInputStream inputStream = null;
		Scanner sc = null;
		
		
		int count = 0;
		System.out.println("TOTAL FILES:"+ files.length);
		
		for(File f : files) {
			count++;
			if(count < 8200)
				continue;
			if(count < 100 && count %10 == 0)
				System.out.println("\n\n============="+count+"============\n\n");
			if(count%100 == 0)
				System.out.println("\n\n============="+count+"============\n\n");
			//System.out.println("processing - " + f);
			String filepath = f.getAbsolutePath();
			inputStream = new FileInputStream(filepath);
			sc = new Scanner(inputStream);
			
			List<String> lines = new ArrayList<String>();
			
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				
				if(line.length() > 0) {
					lines.add(line);
				}
				
			}
			
			inputStream.close();
			//System.out.println("TOTAL LINES: "+lines.size());
			Map<String, List<String>> keyToLines = readLines(lines);
			
			for(String key: keyToLines.keySet()) {
				List<String> entries = keyToLines.get(key);
				
				String firstLine = "";
				String data = "";
				for (String line: entries) {
					
					if(line.trim().isEmpty())
						continue;
					
					data+=line + "\n";
					
					if(firstLine.length() == 0) {
						firstLine = line;
						
					}
					
				}
				
				if(data.trim().isEmpty()) {
					continue;
				}
				
				Block tmp = GalileoConnector.createBlockNoaa(firstLine, data.substring(0, data.length() - 1), fsName, 0,1,4);
				//System.out.println("BLOCK CREATED");
				if (tmp != null) {
					gc.store(tmp);
					//System.out.println("BLOCK STORED");
					Thread.sleep(2);
				}
				
				
			}
			
			//System.out.println("HELLO");
			
		}
		
		
	}




	/**
	 * Reads a file and groups lines by key
	 * @author sapmitra
	 * @return 
	 */
	public static Map<String, List<String>> readLines(List<String> lines) {
		
		Map<String, List<String>> keyToLines = new HashMap<String, List<String>>();
		
		for(String line: lines) {
			
			String observationDate = line.substring(15, 15+8);
			
			String formatted_date = observationDate.substring(0,4)+"-"+observationDate.substring(4,6)+"-"+observationDate.substring(6,8);
			//System.out.println(formatted_date);
			
			String observationTime = line.substring(23, 23+4);
			//System.out.println(observationTime);
			
			String latitude = line.substring(28, 28+6);
			String longitude = line.substring(34, 34+7);
			
			//System.out.println(latitude + " "+ longitude);
			
			double lat = Double.valueOf(latitude)/1000;
			double lng = Double.valueOf(longitude)/1000;
			
			
			String geohash = GeoHash.encode((float)lat, (float)lng, 4);
			//System.out.println(geohash);
			
			String key = formatted_date+"-"+geohash;
			
			//System.out.println(key);
			
			//System.out.println(lat + " "+ lng);
			
			// Accept only FM-12..16,18
			String report_type = line.substring(41,41+5);
			
			if(!"FM-12".equals(report_type) && !"FM-13".equals(report_type) && !"FM-14".equals(report_type)
					&& !"FM-15".equals(report_type) && !"FM-16".equals(report_type) && !"FM-18".equals(report_type)) 
				continue;
			
			//System.out.println(report_type);
			
			int elevation = Integer.valueOf(line.substring(46,46+5));
			//System.out.println(elevation);
			
			
			
			//CALCULATE TIMESTAMP
			
			long timestamp = getTimestamp(observationDate.substring(0,4), observationDate.substring(4,6), observationDate.substring(6,8), 
					observationTime.substring(0,2), observationTime.substring(2,4));
			//WEATHER_STATION_ID
			String weather_station_id = line.substring(51, 51+5);
			
			//WIND-OBSERVATION direction angle
			String wind_dir_angle = line.substring(60,60+3);
			//WIND-OBSERVATION type code
			String wind_dir_typecode = line.substring(64,64+1);
			//WIND-OBSERVATION speed rate
			int wind_speed = Integer.valueOf(line.substring(65,65+4));
			//SKY-CONDITION-OBSERVATION ceiling height dimension
			int sky_ceiling_height = Integer.valueOf(line.substring(70,70+5));
			//SKY-CONDITION-OBSERVATION ceiling determination code
			String sky_ceiling_code = line.substring(76,77);
			//SKY-CONDITION-OBSERVATION CAVOK code 
			String sky_cavoc = line.substring(77,78);
			//VISIBILITY-OBSERVATION distance dimension
			int visibility_dist = Integer.valueOf(line.substring(78,78+6));
			//AIR-TEMPERATURE-OBSERVATION air temperature
			int air_temp = Integer.valueOf(line.substring(87,87+5));
			//AIR-TEMPERATURE-OBSERVATION dew point temperature
			int air_dew_point = Integer.valueOf(line.substring(93,98));
			//ATMOSPHERIC-PRESSURE-OBSERVATION sea level pressure 
			int atm_pressure = Integer.valueOf(line.substring(99,104));
			
			
			String record = lat + ","+ lng + "," + report_type+","+elevation+","+timestamp+","+weather_station_id+","+wind_dir_angle
					+","+wind_dir_typecode+","+wind_speed+","+sky_ceiling_height+","+sky_ceiling_code+","+sky_cavoc+","+visibility_dist+","+air_temp
					+","+air_dew_point+","+atm_pressure;
			
			//System.out.println("HELLO");
			
			List<String> recs = null;
			if(keyToLines.get(key) == null) {
				recs = new ArrayList<String>();
				keyToLines.put(key, recs);
			} else {
				recs = keyToLines.get(key);
			}
			recs.add(record);
			
			
		}
		
		return keyToLines;
		
	}
	
	public static long getTimestamp(String year, String month, String day, String hour, String mins) {
		
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TimeZone.getTimeZone("GMT"));
		c.set(Calendar.DAY_OF_MONTH, Integer.valueOf(day));
		c.set(Calendar.YEAR, Integer.valueOf(year));
		c.set(Calendar.MONTH, Integer.valueOf(month) - 1);
		
		c.set(Calendar.HOUR_OF_DAY, Integer.valueOf(hour));
		c.set(Calendar.MINUTE, Integer.valueOf(mins));
		c.set(Calendar.SECOND, 0);
		
		long baseTime = c.getTimeInMillis();
		
		return baseTime;
	}
	
	
	
	

}
