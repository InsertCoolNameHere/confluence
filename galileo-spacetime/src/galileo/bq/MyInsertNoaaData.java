package galileo.bq;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	/*public static String[] geohashes_2char = {"b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","c0","c1","c2","c3","c4","c5","c6","c7"
			,"c8","c9","bb","bc","bd","be","bf","bg","bh","bk","d0","bn","d1","d2","d3","d4","d5","bs","d6","d7","bu","d8"
			,"d9","bz","cb","cc","cd","ce","cf","cg","ch","ck","cm","e0","e1","cp","e2","cq","e3","cr","cs","e5","e6","ct"
			,"cu","e7","cv","e9","cw","cx","cy","cz","db","dc","dd","de","df","dg","dh","dj","dk","dm","dn","f0","f1","dp"
			,"f2","dq","f3","dr","f4","f5","ds","f6","dt","du","f7","f8","dv","dw","f9","dx","dy","dz","eb","ec","ed","ee"
			,"ef","eh","ej","ek","em","g0","en","g1","ep","g2","eq","g3","g4","er","g5","es","et","g6","g7","eu","ev","g8"
			,"g9","ew","ex","ey","ez","fb","fc","fd","fe","ff","fg","fh","fj","fk","fm","fn","fp","fr","fs","h5","ft","fu"
			,"h7","fv","fw","fx","fz","gb","gc","gd","ge","gf","gg","gh","gk","gn","gp","gq","gr","gs","gt","gu","gv","gw"
			,"gx","gy","hf","hg","j1","j5","j7","j9","hz","k1","k2","k3","k6","k7","k8","k9","jb","jd","je","jg","jp","jw"
			,"jx","kb","kd","ke","kg","kk","km","m0","kn","m2","kp","kq","kr","ks","m5","kt","ku","kv","kw","m9","kx","ky"
			,"kz","n3","n6","mh","mj","mk","mm","mn","mp","mq","mr","mv","mw","mx","my","mz","nd","nh","nk","p4","ns","p7"
			,"p9","q7","q9","pc","pd","pe","pf","pg","ph","r0","r1","r2","r3","r4","r5","r6","r7","pw","px","py","pz","qc"
			,"qd","qe","qf","qg","qj","qm","s0","s1","s2","qp","qq","s3","s4","qr","12","s5","qs","13","14","s6","qt","qu"
			,"s7","qv","s8","qw","s9","qx","qy","qz","rb","rc","rd","re","0c","rh","0f","rj","rk","rm","rn","t0","rp","20"
			,"rq","t4","rr","t5","rs","rt","24","ru","t7","t8","rv","t9","rw","rx","ry","rz","sb","sc","sd","se","sf","sg"
			,"sh","sj","sk","sm","sn","u0","u1","sp","u2","u3","sq","sr","u4","u5","ss","u6","st","u7","su","35","u8","sv"
			,"u9","sw","sx","sy","sz","tb","tc","td","te","tf","tg","2e","th","2g","tj","2h","tk","2j","2k","tm","v0","tn"
			,"v1","2n","v2","tp","tq","v3","v4","2p","tr","v5","ts","2q","tt","v6","tu","v7","2s","v8","tv","2t","tw","v9"
			,"2u","47","tx","2v","ty","tz","2y","ub","uc","ud","ue","uf","ug","3e","uh","uj","uk","um","w0","un","w1","w2"
			,"w3","uq","w4","3p","w5","us","w6","ut","54","w7","uu","w8","56","w9","uw","3u","59","3x","3z","vb","vc","vd"
			,"ve","vf","vg","4e","vh","vk","x0","x1","4m","x2","x3","vq","62","x4","4q","vs","63","x5","4r","64","vt","vu"
			,"4s","x7","x8","66","vv","67","x9","vw","68","69","4w","4x","wb","wc","wd","5b","we","wf","wg","5e","wh","5f"
			,"5g","wj","wk","5j","wm","y0","wn","y1","y2","wp","5n","y3","wq","y4","wr","ws","y5","wt","y6","75","y7","wu"
			,"wv","y8","y9","ww","wx","wy","79","wz","xb","xc","xe","6c","6d","xf","6e","6f","xh","6g","xj","6h","xk","xm"
			,"6k","xn","z0","6m","z1","xp","80","z2","6n","z3","xq","6p","z4","xr","82","83","z5","6q","xs","z6","84","6r"
			,"xt","6s","z7","85","6t","z8","86","xv","z9","6u","xw","87","6v","xx","88","89","xy","6w","xz","6x","6y","6z"
			,"yb","yc","yd","7b","ye","yf","yg","yh","7h","yk","7j","ym","7k","yn","7n","91","yq","7p","93","ys","7q","yt"
			,"94","7r","yu","95","96","yv","7v","7w","yy","7y","7z","zb","zc","zd","8b","8c","ze","zf","8d","zg","8e","8f"
			,"zh","8g","zj","8h","zk","8j","8k","zm","8m","8n","8p","zs","zu","8s","8t","8u","8v","8w","zy","8x","8y","8z"
			,"9b","9d","9e","9f","9g","9h","9j","9m","9n","9p","9q","9r","9s","9t","9u","9v","9w","9x","9y","9z"};*/
	
	public static String[] geohashes_2char = {"b","c","f","g","u","v","y","z",
			"8","9","d","e","s","t","w","x",
			"2","3","6","7","k","m","q","r",
			"0","1","4","5","h","j","n","p"};
	
	public static List<String> validGeoHashes = new ArrayList<String>(Arrays.asList(geohashes_2char));
	
	private static String fsName = "noaafs";
	private static boolean FS_CREATED = true;
	private static int start = 0;
	private static int end = 0;
	public static String baseDir = "/s/lattice-64/a/nobackup/galileo/sapmitra/noaa/ftp.ncdc.noaa.gov/pub/data/noaa/2015";
	//public static String baseDir = "/s/green/a/tmp/sapmitra/noaa_test/2015_test";
	//public static String outputDir = "/s/lattice-64/a/nobackup/galileo/sapmitra/noaa/ftp.ncdc.noaa.gov/pub/data/noaa/2015_classified/";
	//public static String outputDir = "/s/green/a/tmp/sapmitra/noaa_test/2015_classified/";
	//public static List<String> keys = new ArrayList<String>();
	
	public static void main(String arg[]) throws Exception {
		
		//List<String> ln = new ArrayList<String>();
		//ln.add("0083010840999992015010100004+69450+030033FM-12+002799999V0203101N001019999999N999999999-00201-00371999999ADDAA106000091OD139900101999REMSYN04601084 16/// /3101 11020 21037 60001 333 91101=");
		String machine = arg[0];
		FS_CREATED = Boolean.valueOf(arg[1]);
		start = Integer.valueOf(arg[2]);
		end = Integer.valueOf(arg[3]);
		insertFiles(baseDir, machine);
		//readLines(ln);
		
	}
	
	
	
	
	private static void insertFiles(String dirName, String machine) throws Exception {
		
		GalileoConnector gc = new GalileoConnector(machine, 5634);
		
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
			if(!(count>= start && count <= end))
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
					Thread.sleep(1);
				}
				
				
			}
			
			//System.out.println("HELLO");
			Thread.sleep(20);
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
			
			
			String geohash = GeoHash.encode((float)lat, (float)lng, 2);
			
			if(!validGeoHashes.contains(geohash.substring(0, geohash.length() - 1)))
				continue;
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
