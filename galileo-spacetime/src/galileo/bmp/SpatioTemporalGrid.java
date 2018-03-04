package galileo.bmp;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.dataset.Coordinates;
import galileo.dataset.Point;
import galileo.dataset.SpatialRange;
import galileo.util.GeoHash;

public class SpatioTemporalGrid {
	
	private static final Logger logger = Logger.getLogger("galileo");
	private int gridPrecision = 0;
	private String baseHash;
	private SpatialRange baseRange;
	private int width, height;
	private float xDegreesPerPixel;
	private float yDegreesPerPixel;
	
	/**
	 * 
	 * @param baseGeohash
	 * @param spatialPrecision the uncertainty precision specified during file creation
	 */
	public SpatioTemporalGrid(String baseGeohash, int spatialPrecision) {
		
		this.gridPrecision = baseGeohash.length() + 2;
		
		if( this.gridPrecision > spatialPrecision ) {
			this.gridPrecision = spatialPrecision;
		}
		
		
		this.baseRange = GeoHash.decodeHash(baseGeohash);
		this.baseHash = baseGeohash;
		

		this.width = 32; /* = 2^w */
		this.height = 32; /* = 2^h */

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
	public SpatioTemporalGrid() {}
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

		float yDiff = baseRange.getUpperBoundForLatitude() - lat;
		
		int x = (int) (xDiff / xDegreesPerPixel);
		int y = (int) (yDiff / yDegreesPerPixel);

		return new Point<>(x, y);
	}
	
	/**
	 * returns actual index number from gris coordinates
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
	
	
	public List<Integer> getNeighborIndices(int x, int y) {
		List<Integer> neighbors = new ArrayList<Integer>();
		for(int i = -1; i<=1; i++) {
			for(int j= -1; j<=1; j++) {
				int newx = x+i;
				int newy = y+j;
				if(newx > 0 && newy > 0) {
					//System.out.println(newx+" "+newy );
					System.out.println(XYtoIndex(newx, newy));
					neighbors.add(XYtoIndex(newx, newy));
				}
			}
		}
		//neighbors.add(e)
		return null;
		
	}
	
	public static void main(String arg[]) {
		
		SpatioTemporalGrid sg = new SpatioTemporalGrid("9w",6);
		sg.coordinatesToXY(-112.3406f,39.2786f);
		sg.getNeighborIndices(1, 2);
		
	}
	
}
