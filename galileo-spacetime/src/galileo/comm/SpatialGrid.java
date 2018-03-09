package galileo.comm;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.dataset.Coordinates;
import galileo.dataset.Point;
import galileo.dataset.SpatialRange;
import galileo.util.GeoHash;

/**
 * 
 * @author Saptashwa
 *
 */
public class SpatialGrid {
	
	private static final Logger logger = Logger.getLogger("galileo");
	private int gridPrecision = 0;
	private String baseHash;
	private SpatialRange baseRange;
	private int width, height;
	private float xDegreesPerPixel;
	private float yDegreesPerPixel;
	
	private List<List<Integer>> indexEntries;
	
	/**
	 * 
	 * @param baseGeohash
	 * @param spatialPrecision the uncertainty precision specified during file creation
	 */
	public SpatialGrid(String baseGeohash, int spatialPrecision) {
		
		this.gridPrecision = baseGeohash.length() + 2;
		this.width = 32; /* = 2^w */
		this.height = 32; /* = 2^h */
		
		if( this.gridPrecision > spatialPrecision ) {
			this.gridPrecision = spatialPrecision;
			if(baseGeohash.length() % 2 == 0) {
				this.width = 8; /* = 2^w */
				this.height = 4; /* = 2^h */
			} else {
				this.width = 4; /* = 2^w */
				this.height = 8; /* = 2^h */
			}
		}
		
		indexEntries = new ArrayList<List<Integer>>();
		for(int i=0; i< width*height; i++) {
			indexEntries.add(new ArrayList<Integer>());
		}
		
		
		this.baseRange = GeoHash.decodeHash(baseGeohash);
		this.baseHash = baseGeohash;

		/*
		 * Determine the number of degrees in the x and y directions for the
		 * base spatial range this geoavailability grid represents
		 */
		float xDegrees = baseRange.getUpperBoundForLongitude() - baseRange.getLowerBoundForLongitude();
		float yDegrees = baseRange.getUpperBoundForLatitude() - baseRange.getLowerBoundForLatitude();

		/* Determine the number of degrees represented by each grid pixel */
		xDegreesPerPixel = xDegrees / (float) this.width;
		yDegreesPerPixel = yDegrees / (float) this.height;

	}
	public SpatialGrid() {}
	
	/**
	 * Adding an entry to the gridmap
	 * @param lon
	 * @param lat
	 */
	public void addEntry(float lon, float lat, int recNum) {
		
		int index = coordinatesToIndex(lon, lat);
		//System.out.println(recNum+" added to index "+index);
		indexEntries.get(index).add(recNum);
		
	}
	
	public Point<Integer> coordinatesToXY(Coordinates coords) {

		/*
		 * Assuming (x, y) coordinates for the geoavailability grids, latitude
		 * will decrease as y increases, and longitude will increase as x
		 * increases. This is reflected in how we compute the differences
		 * between the base points and the coordinates in question.
		 */
		
		return coordinatesToXY(coords.getLongitude(), coords.getLatitude());
		
	}
	
	public Point<Integer> coordinatesToXY(float lon, float lat) {
		float xDiff = lon - baseRange.getLowerBoundForLongitude();

		float yDiff = lat - baseRange.getLowerBoundForLatitude();
		
		int x = (int) (xDiff / xDegreesPerPixel);
		int y = (int) (yDiff / yDegreesPerPixel);
		//System.out.println("CONVERTED: "+x+" "+y);
		return new Point<>(x, y);
	}
	
	public int coordinatesToIndex(float lon, float lat) {
		float xDiff = lon - baseRange.getLowerBoundForLongitude();

		float yDiff = lat - baseRange.getLowerBoundForLatitude();
		
		int x = (int) (xDiff / xDegreesPerPixel);
		int y = (int) (yDiff / yDegreesPerPixel);
		return XYtoIndex(x, y);
	}
	
	
	/**
	 * returns actual index number from grid coordinates
	 * @author sapmitra
	 * @param x
	 * @param y
	 * @return
	 */
	public int XYtoIndex(int x, int y) {
		return y * this.width + x;
	}

	/**
	 * Converts a bitmap index to X, Y coordinates in the grid.
	 */
	public Point<Integer> indexToXY(int index) {
		int x = index % this.width;
		int y = index / this.width;
		return new Point<>(x, y);
	}
	
	/**
	 * finds all neighboring indices of a given index, including itself
	 * 
	 * @author sapmitra
	 * @param index
	 * @return
	 */
	public List<Integer> getNeighborIndices(int index) {
		Point<Integer> p = indexToXY(index);
		return getNeighborIndices(p.X(),p.Y());
		
	}
	
	public List<Integer> getNeighborIndices(int x, int y) {
		List<Integer> neighbors = new ArrayList<Integer>();
		for(int i = -1; i<=1; i++) {
			for(int j= -1; j<=1; j++) {
				int newx = x+i;
				int newy = y+j;
				if(newx >= 0 && newy >= 0) {
					//System.out.println(newx+" "+newy );
					//System.out.println(XYtoIndex(newx, newy));
					neighbors.add(XYtoIndex(newx, newy));
				}
			}
		}
		//neighbors.add(e)
		return null;
		
	}
	
	public static void main(String arg[]) {
		
		SpatialGrid sg = new SpatialGrid("9x",6);
		sg.coordinatesToXY(-110.2006f,41.337f);//9x3
		sg.getNeighborIndices(1, 2);//9x9
		
		sg.addEntry(-110.585f, 39.629f, 11);//9x13
		sg.addEntry(-110.513f, 39.654f, 21);//9x13
		sg.addEntry(-112.062f, 39.595f, 12);//9x03
		
		JSONObject jsonStringRepresentation = sg.getJsonStringRepresentation("abcd");
		System.out.println(jsonStringRepresentation);
		SpatialGrid sg1 = new SpatialGrid();
		sg1.populateObject(jsonStringRepresentation);
		System.out.println("Hello");
		//System.out.println(sg.indexEntries.get(33));
		//System.out.println(sg.indexEntries.get(37));
		
		System.out.println(sg.XYtoIndex(13, 2));
		System.out.println(sg.indexToXY(77));
		
	}
	public List<List<Integer>> getIndexEntries() {
		return indexEntries;
	}
	public void setIndexEntries(List<List<Integer>> indexEntries) {
		this.indexEntries = indexEntries;
	}
	
	
	/**
	 * CONVERTING CURRENT OBJECT INTO A STRING
	 * @param path
	 * @return
	 */
	public JSONObject getJsonStringRepresentation(String path) {
		JSONObject sMap = new JSONObject();
		
		sMap.put("blockName", path);
		sMap.put("gridPrecision",gridPrecision);
		sMap.put("baseHash",baseHash);
		sMap.put("width",width);
		sMap.put("height",height);
		sMap.put("xDegreesPerPixel",xDegreesPerPixel);
		sMap.put("yDegreesPerPixel",yDegreesPerPixel);
		
		sMap.put("indexEntries",indexEntries);
		
		return sMap;
	}
	
	public void populateObject(JSONObject jsonObj) {
		
		this.gridPrecision = jsonObj.getInt("gridPrecision");
		this.baseHash = jsonObj.getString("baseHash");
		this.width = jsonObj.getInt("width");
		this.height = jsonObj.getInt("height");
		this.xDegreesPerPixel = (float)jsonObj.getDouble("xDegreesPerPixel");
		this.yDegreesPerPixel = (float)jsonObj.getDouble("yDegreesPerPixel");
		
		JSONArray indexEntriess = jsonObj.getJSONArray("indexEntries");
		indexEntries = new ArrayList<List<Integer>>();
		
		for (int i = 0; i < indexEntriess.length(); i++){
			JSONArray ss = (JSONArray)indexEntriess.get(i);
			List<Integer> grdEntries = new ArrayList<Integer>();
			
			for (int j = 0; j < ss.length(); j++)
				grdEntries.add(ss.getInt(j));
			
			indexEntries.add(grdEntries);
		}
			//indexEntries.add(indexEntriess.getInt(0));
		
	}
}
