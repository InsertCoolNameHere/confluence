package galileo.bmp;

import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.util.GeoHash;

public class SpatioTemporalGrid {
	
	private static final Logger logger = Logger.getLogger("galileo");
	private String baseHash;
	private String baseTime;
	
	public SpatioTemporalGrid(String baseGeohash, int precision) {
		this.baseRange = GeoHash.decodeHash(baseGeohash);
		this.baseHash = baseGeohash;
		/*
		 * height, width calculated like so: width = 2^(floor(precision / 2))
		 * height = 2^(ceil(precision / 2))
		 */
		int w = precision / 2;
		int h = precision / 2;
		if (precision % 2 != 0) {
			h += 1;
		}

		this.width = (1 << w); /* = 2^w */
		this.height = (1 << h); /* = 2^h */

		/*
		 * Determine the number of degrees in the x and y directions for the
		 * base spatial range this geoavailability grid represents
		 */
		float xDegrees = baseRange.getUpperBoundForLongitude() - baseRange.getLowerBoundForLongitude();
		float yDegrees = baseRange.getUpperBoundForLatitude() - baseRange.getLowerBoundForLatitude();

		/* Determine the number of degrees represented by each grid pixel */
		xDegreesPerPixel = xDegrees / (float) this.width;
		yDegreesPerPixel = yDegrees / (float) this.height;

		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO,
					"Created geoavailability grid: " + "geohash={0}, precision={1}, "
							+ "width={2}, height={3}, baseRange={6}, " + "xDegreesPerPixel={4}, yDegreesPerPixel={5}",
					new Object[] { baseGeohash, precision, width, height, xDegreesPerPixel, yDegreesPerPixel,
							baseRange });
		}
	}
	
}
