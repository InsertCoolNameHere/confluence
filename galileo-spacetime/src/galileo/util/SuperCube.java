package galileo.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import galileo.comm.TemporalType;
import galileo.dataset.Coordinates;
import galileo.dht.hash.TemporalHash;
import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class SuperCube implements ByteSerializable{
	
	// These are blocks of fs1 that require this supercube.
	private long id;
	private List<String> fs1BlockPaths;
	private List<Coordinates> polygon;
	/* time contains two timestamps separated by a - */
	private String time;
	private String centralgeohash;
	private String centralTime;
	
	@Override
	public String toString() {
		return id+" "+centralgeohash+" "+polygon+" "+centralTime+" "+time;
	}
	
	public List<Coordinates> getPolygon() {
		return polygon;
	}
	public void setPolygon(List<Coordinates> polygon) {
		this.polygon = polygon;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	
	public List<String> getFs1BlockPath() {
		return fs1BlockPaths;
	}
	public void setFs1BlockPath(List<String> fs1BlockPath) {
		this.fs1BlockPaths = fs1BlockPath;
	}
	
	public void addFs1Path(String path) {
		if(fs1BlockPaths == null) {
			fs1BlockPaths = new ArrayList<String>();
			fs1BlockPaths.add(path);
			return;
		}
		for(String p: fs1BlockPaths) {
			if(path.equals(p)) {
				return;
			}
		}
		fs1BlockPaths.add(path);
	}
	
	
	public String getCentralGeohash() {
		return centralgeohash;
	}
	public void setCentralGeohash(String geohash) {
		this.centralgeohash = geohash;
	}
	
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeLong(id);
		if(fs1BlockPaths != null && fs1BlockPaths.size() > 0) {
			out.writeBoolean(true);
			out.writeStringCollection(fs1BlockPaths);
		} else {
			out.writeBoolean(false);
		}
		
		if(polygon != null && polygon.size() > 0) {
			out.writeBoolean(true);
			out.writeSerializableCollection(polygon);
		} else {
			out.writeBoolean(false);
		}
		
		if(this.time != null && !this.time.isEmpty()) {
			out.writeBoolean(true);
			out.writeString(time);
		} else {
			out.writeBoolean(false);
		}
		
		out.writeString(centralgeohash);
		
		if(this.centralTime != null && !this.centralTime.isEmpty()) {
			out.writeBoolean(true);
			out.writeString(centralTime);
		} else {
			out.writeBoolean(false);
		}
		
	}
	
	@Deserialize
	public SuperCube(SerializationInputStream in) throws IOException, SerializationException {
		this.id = in.readLong();
		
		boolean hasfs1BlockPaths = in.readBoolean();
		if(hasfs1BlockPaths) {
			fs1BlockPaths = new ArrayList<String>();
			in.readStringCollection(this.fs1BlockPaths);
		}
		
		boolean hasPolygon = in.readBoolean();
		if(hasPolygon) {
			this.polygon = new ArrayList<Coordinates>();
			in.readSerializableCollection(Coordinates.class, this.polygon);
		}
		
		boolean hasTimeString = in.readBoolean();
		
		if(hasTimeString) {
			this.time = in.readString();
		}
		
		this.centralgeohash = in.readString();
		
		boolean hasCentralTimeString = in.readBoolean();
		
		if(hasCentralTimeString) {
			this.centralTime = in.readString();
		}
		
	}
	public SuperCube() {}
	public String getCentralTime() {
		return centralTime;
	}
	public void setCentralTime(String centralTime) {
		this.centralTime = centralTime;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	
	/**
	 * 
	 * @author sapmitra
	 * @param supercubeToNodeMap
	 * @param superCubeNumNodesMap
	 * @param key : node string
	 * @param index
	 */
	public static void addToCubeNodeMap(Map<Integer, List<String>> supercubeToNodeMap, Map<Integer, Integer> superCubeNumNodesMap, String key, int index) {
		
		List<String> nodesQueried = supercubeToNodeMap.get(index);
		int numNodes = 0;
		if(superCubeNumNodesMap.get(index) != null) {
			numNodes = superCubeNumNodesMap.get(index);
		}
		
		if(nodesQueried == null) {
			nodesQueried = new ArrayList<String>();
			nodesQueried.add(key);
			supercubeToNodeMap.put(index, nodesQueried);
			superCubeNumNodesMap.put(index,1);
			return;
		} else {
			
			if(!nodesQueried.contains(key)) {
				nodesQueried.add(key);
				numNodes++;
				superCubeNumNodesMap.put(index,numNodes);
			}
		}
		
		
		
	}
	
	/**
	 * Returns the dates that lie between two timestamps
	 * 
	 * @author sapmitra
	 * @param time
	 * @return
	 */
	public static List<Date> handleTemporalRangeForEachBlock(String time) {
		// TODO Auto-generated method stub
		if(time != null && time.contains("-")) {
			String[] tokens = time.split("-");
			
			long start = Long.parseLong(tokens[0]);
			long end = Long.parseLong(tokens[1]);
			
			Calendar cs = Calendar.getInstance();
			cs.setTimeZone(TemporalHash.TIMEZONE);
			cs.setTimeInMillis(start);
			
			Calendar ce = Calendar.getInstance();
			ce.setTimeZone(TemporalHash.TIMEZONE);
			ce.setTimeInMillis(end);
			
			Date ds = cs.getTime();
			Date de = ce.getTime();
			
			List<Date> daysBetweenDates = getDaysBetweenDates(ds,de);
			
			// We can change dimensionality using the days if we want
			
			System.out.println(daysBetweenDates);
			
			return daysBetweenDates;
			
		} else {
			return null;
		}
	}
	


	private static List<Date> getDaysBetweenDates(Date startdate, Date enddate) {
		
		Calendar cend = Calendar.getInstance();
	    cend.setTimeZone(TemporalHash.TIMEZONE);
	    cend.setTime(enddate);
	    
	    
	    List<Date> dates = new ArrayList<Date>();
	    Calendar calendar = Calendar.getInstance();
	    calendar.setTimeZone(TemporalHash.TIMEZONE);
	    calendar.setTime(startdate);

	    while (calendar.getTime().before(enddate))
	    {
	        Date result = calendar.getTime();
	        dates.add(result);
	        calendar.add(Calendar.DATE, 1);
	    }
	    
	    if(calendar.getTimeInMillis() > cend.getTimeInMillis()) {
	    	dates.add(cend.getTime());
	    }
	    return dates;
	}
	
	public static void main(String arg[]) {
		SuperCube.addToCubeNodeMap(new HashMap<Integer, List<String>> (), new HashMap<Integer, Integer> (), "sdsd", 1);
	}
	
	/*public static void main(String arg[]) throws IOException {
		
		SuperCube s = new SuperCube();
		
		s.setDateString("abc");
		
		ByteOutputStream baos = new ByteOutputStream();
		
		SerializationOutputStream so = new SerializationOutputStream(baos);
		
		s.serialize(so);
		
		so.
		
		System.out.println("Hello");
		
		so.
		
		ByteInputStream bin = new ByteInputStream();
		
		SerializationInputStream si = new SerializationInputStream(bin);
		
		
		
		
		
	}*/
}
