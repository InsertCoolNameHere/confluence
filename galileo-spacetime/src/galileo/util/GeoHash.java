/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.util;

import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.geom.Area;
import java.awt.geom.Rectangle2D;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;

import galileo.comm.TemporalType;
import galileo.dataset.Coordinates;
import galileo.dataset.Point;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dht.Partitioner;
import galileo.dht.hash.TemporalHash;
import galileo.fs.GeospatialFileSystem;
import math.geom2d.Point2D;
import math.geom2d.polygon.SimplePolygon2D;

/**
 * This class provides an implementation of the GeoHash (http://www.geohash.org)
 * algorithm.
 *
 * See http://en.wikipedia.org/wiki/Geohash for implementation details.
 */
public class GeoHash {

	public final static byte BITS_PER_CHAR = 5;
	public final static int LATITUDE_RANGE = 90;
	public final static int LONGITUDE_RANGE = 180;
	public final static int MAX_PRECISION = 30; // 6 character precision = 30 (~
												// 1.2km x 0.61km)

	/**
	 * This character array maps integer values (array indices) to their GeoHash
	 * base32 alphabet equivalents.
	 */
	public final static char[] charMap = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
			'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

	/**
	 * Allows lookups from a GeoHash character to its integer index value.
	 */
	public final static HashMap<Character, Integer> charLookupTable = new HashMap<Character, Integer>();

	/**
	 * Initialize HashMap for character to integer lookups.
	 */
	static {
		for (int i = 0; i < charMap.length; ++i) {
			charLookupTable.put(charMap[i], i);
		}
	}

	private String binaryHash;
	private Rectangle2D bounds;

	public GeoHash() {
		this("");
	}

	public GeoHash(String binaryString) {
		this.binaryHash = binaryString;
		ArrayList<Boolean> bits = new ArrayList<>();
		for (char bit : this.binaryHash.toCharArray())
			bits.add(bit == '0' ? false : true);
		float[] longitude = decodeBits(bits, false);
		float[] latitude = decodeBits(bits, true);
		SpatialRange range = new SpatialRange(latitude[0], latitude[1], longitude[0], longitude[1]);
		Pair<Coordinates, Coordinates> coordsPair = range.get2DCoordinates();
		Point<Integer> upLeft = coordinatesToXY(coordsPair.a);
		Point<Integer> lowRight = coordinatesToXY(coordsPair.b);
		this.bounds = new Rectangle(upLeft.X(), upLeft.Y(), lowRight.X() - upLeft.X(), lowRight.Y() - upLeft.Y());
	}

	public int getPrecision() {
		return this.binaryHash.length();
	}

	public String getBinaryHash() {
		return this.binaryHash;
	}

	public String[] getValues(int precision) {
		String[] values = null;
		String hash = "";
		for (int i = 0; i < this.binaryHash.length(); i += 5) {
			String hashChar = this.binaryHash.substring(i, java.lang.Math.min(i + 5, this.binaryHash.length()));
			if (hashChar.length() == 5)
				hash += charMap[Integer.parseInt(hashChar, 2)];
			else {
				String beginHash = hashChar;
				String endHash = hashChar;
				while (beginHash.length() < BITS_PER_CHAR) {
					beginHash += "0";
					endHash += "1";
				}
				values = new String[2];
				values[0] = hash + charMap[Integer.parseInt(beginHash, 2)];
				values[1] = hash + charMap[Integer.parseInt(endHash, 2)];
				while (values[0].length() < precision){
					values[0] += "0";
					values[1] += "z";
				}
			}
		}
		if (values == null){
			if (hash.length() < precision){
				String beginHash = hash;
				String endHash = hash;
				while (beginHash.length() < precision){
					beginHash += "0";
					endHash += "z";
				}
				values = new String[] { beginHash, endHash };
			} else {
				values = new String[] {hash};
			}
		}
		return values;
	}

	public Rectangle2D getRectangle() {
		return this.bounds;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GeoHash) {
			GeoHash other = (GeoHash) obj;
			return this.binaryHash.equals(other.binaryHash);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.binaryHash.hashCode();
	}

	
	/**
	 * 
	 * @author sapmitra
	 * @param year
	 * @param month
	 * @param day
	 * @param hour
	 * @param temporalType
	 * @return
	 * @throws ParseException
	 */
	public static long getStartTimeStamp(String year, String month, String day, String hour, TemporalType temporalType) throws ParseException {
		
		Calendar calendar = Calendar.getInstance(TemporalHash.TIMEZONE);
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		//calendar.setLenient(false);
		
		
		if(month.contains("x"))
			month = "00";
		if(day.contains("x"))
			day = "01";
		if(hour.contains("x"))
			hour = "00";
		
		switch (temporalType) {
			case HOUR_OF_DAY:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
				calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
				calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(hour));
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			case DAY_OF_MONTH:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
				calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			case MONTH:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
				calendar.set(Calendar.DAY_OF_MONTH, 1);
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			case YEAR:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, 0);
				calendar.set(Calendar.DAY_OF_MONTH, 1);
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			    
		}
		
	        
	    long offset = TemporalHash.TIMEZONE.getRawOffset() - TimeZone.getDefault().getRawOffset();
	    
	    
	    //System.out.println(calendar.getTimeInMillis() + offset);
	    return calendar.getTimeInMillis() + offset;
		
	}

	/**
	 * 
	 * @author sapmitra
	 * @param year
	 * @param month
	 * @param day
	 * @param hour
	 * @param temporalType
	 * @return
	 */
	public static long getEndTimeStamp(String year, String month, String day, String hour, TemporalType temporalType) {
		
		Calendar calendar = Calendar.getInstance(TemporalHash.TIMEZONE);
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		//calendar.setLenient(false);
		
		if(month.contains("x"))
			month = "11";
		
		int mn = Integer.parseInt(month);
		
		if(day.contains("x")) {
			if(mn == 1 || mn == 3 || mn == 5 || mn == 7 || mn == 8 || mn == 10 || mn == 12 )
				day = "31";
			else if(mn == 2) {
				int yr = Integer.parseInt(year) ;
				if(yr % 4 == 0) 
					day = "29";
				else
					day = "28";
			}
		}
			
		if(hour.contains("x"))
			hour = "23";
		
		switch (temporalType) {
			case HOUR_OF_DAY:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
				calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
				calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(hour));
				calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			case DAY_OF_MONTH:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
				calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
				calendar.set(Calendar.HOUR_OF_DAY, 23);
				calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			case MONTH:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, Integer.parseInt(month)-1);
				
				int m = Integer.parseInt(month);
				if(m == 1 || m == 3 || m == 5 || m == 7 || m == 8 || m == 10 || m == 12 )
					calendar.set(Calendar.DAY_OF_MONTH, 31);
				else if(m == 2) {
					int yr = calendar.get(Calendar.YEAR) ;
					if(yr % 4 == 0) 
						calendar.set(Calendar.DAY_OF_MONTH, 29);
					else
						calendar.set(Calendar.DAY_OF_MONTH, 28);
				} else 
					calendar.set(Calendar.DAY_OF_MONTH, 30);
				
				calendar.set(Calendar.HOUR_OF_DAY, 23);
			    calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			case YEAR:
				calendar.set(Calendar.YEAR, Integer.parseInt(year));
				calendar.set(Calendar.MONTH, 11);
				calendar.set(Calendar.DAY_OF_MONTH, 31);
				calendar.set(Calendar.HOUR_OF_DAY, 23);
				calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			    
		}
		
	    //System.out.println(calendar.getTime());
		    
	    long offset = TemporalHash.TIMEZONE.getRawOffset() - TimeZone.getDefault().getRawOffset();
	    
	    Date d1 = new Date(calendar.getTimeInMillis() + offset);
	    //System.out.println("CONVERTED:" + d1);
	    
	    //System.out.println(calendar.getTimeInMillis() + offset);
	    return calendar.getTimeInMillis() + offset;
		
	}

	/**
	 * Encode a set of {@link Coordinates} into a GeoHash string.
	 *
	 * @param coords
	 *            Coordinates to get GeoHash for.
	 *
	 * @param precision
	 *            Desired number of characters in the returned GeoHash String.
	 *            More characters means more precision.
	 *
	 * @return GeoHash string.
	 */
	public static String encode(Coordinates coords, int precision) {
		return encode(coords.getLatitude(), coords.getLongitude(), precision);
	}

	/**
	 * Encode {@link SpatialRange} into a GeoHash string.
	 *
	 * @param range
	 *            SpatialRange to get GeoHash for.
	 *
	 * @param precision
	 *            Number of characters in the returned GeoHash String. More
	 *            characters is more precise.
	 *
	 * @return GeoHash string.
	 */
	public static String encode(SpatialRange range, int precision) {
		Coordinates rangeCoords = range.getCenterPoint();
		return encode(rangeCoords.getLatitude(), rangeCoords.getLongitude(), precision);
	}

	/**
	 * Encode latitude and longitude into a GeoHash string.
	 *
	 * @param latitude
	 *            Latitude coordinate, in degrees.
	 *
	 * @param longitude
	 *            Longitude coordinate, in degrees.
	 *
	 * @param precision
	 *            Number of characters in the returned GeoHash String. More
	 *            characters is more precise.
	 *
	 * @return resulting GeoHash String.
	 */
	public static String encode(float latitude, float longitude, int precision) {
		while (latitude < -90f || latitude > 90f)
			latitude = latitude < -90f ? 180.0f + latitude : latitude > 90f ? -180f + latitude : latitude;
		while (longitude < -180f || longitude > 180f)
			longitude = longitude < -180f ? 360.0f + longitude : longitude > 180f ? -360f + longitude : longitude;
		/*
		 * Set up 2-element arrays for longitude and latitude that we can flip
		 * between while encoding
		 */
		float[] high = new float[2];
		float[] low = new float[2];
		float[] value = new float[2];

		high[0] = LONGITUDE_RANGE;
		high[1] = LATITUDE_RANGE;
		low[0] = -LONGITUDE_RANGE;
		low[1] = -LATITUDE_RANGE;
		value[0] = longitude;
		value[1] = latitude;

		String hash = "";

		for (int p = 0; p < precision; ++p) {

			float middle = 0.0f;
			int charBits = 0;
			for (int b = 0; b < BITS_PER_CHAR; ++b) {
				int bit = (p * BITS_PER_CHAR) + b;

				charBits <<= 1;

				middle = (high[bit % 2] + low[bit % 2]) / 2;
				if (value[bit % 2] > middle) {
					charBits |= 1;
					low[bit % 2] = middle;
				} else {
					high[bit % 2] = middle;
				}
			}

			hash += charMap[charBits];
		}

		return hash;
	}

	/**
	 * Convert a GeoHash String to a long integer.
	 *
	 * @param hash
	 *            GeoHash String to convert.
	 *
	 * @return The GeoHash as a long integer.
	 */
	public static long hashToLong(String hash) {
		long longForm = 0;

		/* Long can fit 12 GeoHash characters worth of precision. */
		if (hash.length() > 12) {
			hash = hash.substring(0, 12);
		}

		for (char c : hash.toCharArray()) {
			longForm <<= BITS_PER_CHAR;
			longForm |= charLookupTable.get(c);
		}

		return longForm;
	}

	/**
	 * Decode a GeoHash to an approximate bounding box that contains the
	 * original GeoHashed point.
	 *
	 * @param geoHash
	 *            GeoHash string
	 *
	 * @return Spatial Range (bounding box) of the GeoHash.
	 */
	public static SpatialRange decodeHash(String geoHash) {
		ArrayList<Boolean> bits = getBits(geoHash);

		float[] longitude = decodeBits(bits, false);
		float[] latitude = decodeBits(bits, true);

		return new SpatialRange(latitude[0], latitude[1], longitude[0], longitude[1]);
	}

	/**
	 * @param geohash
	 *            - geohash of the region for which the neighbors are needed
	 * @param direction
	 *            - one of nw, n, ne, w, e, sw, s, se
	 * @return
	 */
	public static String getNeighbour(String geohash, String direction) {
		if (geohash == null || geohash.trim().length() == 0)
			throw new IllegalArgumentException("Invalid Geohash");
		geohash = geohash.trim();
		int precision = geohash.length();
		SpatialRange boundingBox = decodeHash(geohash);
		Coordinates centroid = boundingBox.getCenterPoint();
		float widthDiff = boundingBox.getUpperBoundForLongitude() - centroid.getLongitude();
		float heightDiff = boundingBox.getUpperBoundForLatitude() - centroid.getLatitude();
		switch (direction) {
		case "nw":
			return encode(boundingBox.getUpperBoundForLatitude() + heightDiff,
					boundingBox.getLowerBoundForLongitude() - widthDiff, precision);
		case "n":
			return encode(boundingBox.getUpperBoundForLatitude() + heightDiff, centroid.getLongitude(), precision);
		case "ne":
			return encode(boundingBox.getUpperBoundForLatitude() + heightDiff,
					boundingBox.getUpperBoundForLongitude() + widthDiff, precision);
		case "w":
			return encode(centroid.getLatitude(), boundingBox.getLowerBoundForLongitude() - widthDiff, precision);
		case "e":
			return encode(centroid.getLatitude(), boundingBox.getUpperBoundForLongitude() + widthDiff, precision);
		case "sw":
			return encode(boundingBox.getLowerBoundForLatitude() - heightDiff,
					boundingBox.getLowerBoundForLongitude() - widthDiff, precision);
		case "s":
			return encode(boundingBox.getLowerBoundForLatitude() - heightDiff, centroid.getLongitude(), precision);
		case "se":
			return encode(boundingBox.getLowerBoundForLatitude() - heightDiff,
					boundingBox.getUpperBoundForLongitude() + widthDiff, precision);
		default:
			return "";
		}
	}

	/**
	 * 
	 * Neighbors are returned in the order nw,n,ne,w,e,sw,s,se
	 * @param geoHash
	 * @return
	 */
	public static String[] getNeighbours(String geoHash) {
		String[] neighbors = new String[8];
		if (geoHash == null || geoHash.trim().length() == 0)
			throw new IllegalArgumentException("Invalid Geohash");
		geoHash = geoHash.trim();
		int precision = geoHash.length();
		SpatialRange boundingBox = decodeHash(geoHash);
		Coordinates centroid = boundingBox.getCenterPoint();
		float widthDiff = boundingBox.getUpperBoundForLongitude() - centroid.getLongitude();
		float heightDiff = boundingBox.getUpperBoundForLatitude() - centroid.getLatitude();
		neighbors[0] = encode(boundingBox.getUpperBoundForLatitude() + heightDiff,
				boundingBox.getLowerBoundForLongitude() - widthDiff, precision);
		neighbors[1] = encode(boundingBox.getUpperBoundForLatitude() + heightDiff, centroid.getLongitude(), precision);
		neighbors[2] = encode(boundingBox.getUpperBoundForLatitude() + heightDiff,
				boundingBox.getUpperBoundForLongitude() + widthDiff, precision);
		neighbors[3] = encode(centroid.getLatitude(), boundingBox.getLowerBoundForLongitude() - widthDiff, precision);
		neighbors[4] = encode(centroid.getLatitude(), boundingBox.getUpperBoundForLongitude() + widthDiff, precision);
		neighbors[5] = encode(boundingBox.getLowerBoundForLatitude() - heightDiff,
				boundingBox.getLowerBoundForLongitude() - widthDiff, precision);
		neighbors[6] = encode(boundingBox.getLowerBoundForLatitude() - heightDiff, centroid.getLongitude(), precision);
		neighbors[7] = encode(boundingBox.getLowerBoundForLatitude() - heightDiff,
				boundingBox.getUpperBoundForLongitude() + widthDiff, precision);
		return neighbors;
	}

	/**
	 * @param coordinates
	 *            - latitude and longitude values
	 * @return Point - x, y pair obtained from a geohash precision of 12. x,y
	 *         values range from [0, 4096)
	 */
	public static Point<Integer> coordinatesToXY(Coordinates coords) {
		int width = 1 << MAX_PRECISION;
		float xDiff = coords.getLongitude() + 180;
		float yDiff = 90 - coords.getLatitude();
		int x = (int) (xDiff * width / 360);
		int y = (int) (yDiff * width / 180);
		return new Point<>(x, y);
	}
	
	
	public static Coordinates xyToCoordinates(int x, int y) {
		int width = 1 << MAX_PRECISION;
		return new Coordinates(90 - y * 180f / width, x * 360f / width - 180f);
	}

	/**
	 * 
	 * @param polygon bounding polygon
	 * @return
	 */
	public static String[] getIntersectingGeohashes(List<Coordinates> polygon) {
		Set<String> hashes = new HashSet<String>();
		Polygon geometry = new Polygon();
		
		// Getting MBR out of the POLYGON in geometry object
		for (Coordinates coords : polygon) {
			Point<Integer> point = coordinatesToXY(coords);
			geometry.addPoint(point.X(), point.Y());
		}
		// center may not lie inside polygon so start with any vertex of the
		// polygon
		Coordinates spatialCenter = polygon.get(0);
		
		// GETS MBR WITH XY COORDINATES FOR THE FULL POLYGON
		Rectangle2D box = geometry.getBounds2D();
		
		// Take any point(0) from the polygon and convert it to a 2 character geohash
		
		String geohash = encode(spatialCenter, Partitioner.SPATIAL_PRECISION);
		Queue<String> hashQue = new LinkedList<String>();
		Set<String> computedHashes = new HashSet<String>();
		
		/* Start by inserting this initial geohash for point0 into the queue */
		hashQue.offer(geohash);
		
		while (!hashQue.isEmpty()) {
			String hash = hashQue.poll();
			computedHashes.add(hash);
			/* Returns the bounds for this geohash in terms of latlong */
			SpatialRange hashRange = decodeHash(hash);
			/* Get the top left and bottom right points as coordinates */
			Pair<Coordinates, Coordinates> coordsPair = hashRange.get2DCoordinates();
			Point<Integer> upLeft = coordinatesToXY(coordsPair.a);
			Point<Integer> lowRight = coordinatesToXY(coordsPair.b);
			Rectangle2D hashRect = new Rectangle(upLeft.X(), upLeft.Y(), lowRight.X() - upLeft.X(),
					lowRight.Y() - upLeft.Y());
			/* If the geohash fully encloses the MBR */
			/* box corresponds to the entire polygon */
			if (hash.equals(geohash) && hashRect.contains(box)) {
				hashes.add(hash);
				break;
			}
			/* If it does not fully enclose */
			if (geometry.intersects(hashRect)) {
				hashes.add(hash);
				String[] neighbors = getNeighbours(hash);
				for (String neighbour : neighbors)
					if (!computedHashes.contains(neighbour) && !hashQue.contains(neighbour))
						hashQue.offer(neighbour);
			}
		}
		return hashes.size() > 0 ? hashes.toArray(new String[hashes.size()]) : new String[] {};
	}
	
	/**
	 * Gives the list of geohashes of a required precision that intersect with a given polygon
	 * Use this if you are sure your polygon is a convex polygon
	 * 
	 * @author sapmitra
	 * @param polygon
	 * @param precision
	 * @return
	 */
	public static String[] getIntersectingGeohashesForConvexBoundingPolygon(List<Coordinates> polygon, int precision) {
		Set<String> hashes = new HashSet<String>();
		Polygon geometry = new Polygon();
		for (Coordinates coords : polygon) {
			Point<Integer> point = coordinatesToXY(coords);
			geometry.addPoint(point.X(), point.Y());
		}
		Coordinates spatialCenter = new SpatialRange(polygon).getCenterPoint();
		Rectangle2D box = geometry.getBounds2D();
		String geohash = encode(spatialCenter, precision);
		Queue<String> hashQue = new LinkedList<String>();
		Set<String> computedHashes = new HashSet<String>();
		hashQue.offer(geohash);
		while (!hashQue.isEmpty()) {
			String hash = hashQue.poll();
			computedHashes.add(hash);
			SpatialRange hashRange = decodeHash(hash);
			Pair<Coordinates, Coordinates> coordsPair = hashRange.get2DCoordinates();
			Point<Integer> upLeft = coordinatesToXY(coordsPair.a);
			Point<Integer> lowRight = coordinatesToXY(coordsPair.b);
			Rectangle2D hashRect = new Rectangle(upLeft.X(), upLeft.Y(), lowRight.X() - upLeft.X(),
					lowRight.Y() - upLeft.Y());
			if (hash.equals(geohash) && hashRect.contains(box)) {
				hashes.add(hash);
				break;
			} 
			if (geometry.intersects(hashRect)) {
				hashes.add(hash);
				String[] neighbors = getNeighbours(hash);
				for (String neighbour : neighbors)
					if (!computedHashes.contains(neighbour) && !hashQue.contains(neighbour))
						hashQue.offer(neighbour);
			}
		}
		return hashes.size() > 0 ? hashes.toArray(new String[hashes.size()]) : new String[] {};
	}
	
	
	
	
	/* Same as the previous one. This only lets you add a precision */
	public static String[] getIntersectingGeohashes(List<Coordinates> polygon, int userPrecision) {
		Set<String> hashes = new HashSet<String>();
		Polygon geometry = new Polygon();
		for (Coordinates coords : polygon) {
			Point<Integer> point = coordinatesToXY(coords);
			geometry.addPoint(point.X(), point.Y());
		}
		// center may not lie inside polygon so start with any vertex of the
		// polygon
		Coordinates spatialCenter = polygon.get(0);
		
		// GETS MBR WITH XY COORDINATES
		Rectangle2D box = geometry.getBounds2D();
		
		// Take any point(0) from the polygon and convert it to a 2 character geohash
		
		String geohash = encode(spatialCenter, userPrecision);
		Queue<String> hashQue = new LinkedList<String>();
		Set<String> computedHashes = new HashSet<String>();
		hashQue.offer(geohash);
		while (!hashQue.isEmpty()) {
			String hash = hashQue.poll();
			computedHashes.add(hash);
			/* Returns the bounds for this geohash in terms of latlong */
			SpatialRange hashRange = decodeHash(hash);
			/* Get the top left and bottom right points as coordinates */
			Pair<Coordinates, Coordinates> coordsPair = hashRange.get2DCoordinates();
			Point<Integer> upLeft = coordinatesToXY(coordsPair.a);
			Point<Integer> lowRight = coordinatesToXY(coordsPair.b);
			Rectangle2D hashRect = new Rectangle(upLeft.X(), upLeft.Y(), lowRight.X() - upLeft.X(),
					lowRight.Y() - upLeft.Y());
			/* If the geohash fully encloses the MBR */
			if (hash.equals(geohash) && hashRect.contains(box)) {
				hashes.add(hash);
				break;
			}
			/* If it does not fully enclose */
			if (geometry.intersects(hashRect)) {
				hashes.add(hash);
				String[] neighbors = getNeighbours(hash);
				for (String neighbour : neighbors)
					if (!computedHashes.contains(neighbour) && !hashQue.contains(neighbour))
						hashQue.offer(neighbour);
			}
		}
		return hashes.size() > 0 ? hashes.toArray(new String[hashes.size()]) : new String[] {};
	}
	
	
	/**
	 * 
	 * @author sapmitra
	 * @param polygon
	 * @param flank
	 * @return
	 */
	public static boolean checkIntersection(List<Coordinates> polygon, List<Coordinates> flank) {
		Polygon geometry = new Polygon();
		for (Coordinates coords : polygon) {
			Point<Integer> point = coordinatesToXY(coords);
			geometry.addPoint(point.X(), point.Y());
		}
		
		Polygon geometryFlank = new Polygon();
		for (Coordinates coords : flank) {
			Point<Integer> point = coordinatesToXY(coords);
			geometryFlank.addPoint(point.X(), point.Y());
		}
		
		Area a1 = new Area(geometry);
		Area a2 = new Area(geometryFlank);
		
		a1.intersect(a2);
		
		if(a1.isEmpty()) {
			return false;
		} else {
			return true;
		}
		
	}
	
	/**
	 * returns true if the flank lies completely inside polygon
	 * @author sapmitra
	 * @param polygon
	 * @param flank
	 * @return
	 */
	public static boolean checkEnclosure(List<Coordinates> polygon, List<Coordinates> flank) {
		Polygon geometry = new Polygon();
		for (Coordinates coords : polygon) {
			Point<Integer> point = coordinatesToXY(coords);
			geometry.addPoint(point.X(), point.Y());
		}
		
		Polygon geometrySmaller = new Polygon();
		for (Coordinates coords : flank) {
			Point<Integer> point = coordinatesToXY(coords);
			geometrySmaller.addPoint(point.X(), point.Y());
		}
		Area aPrev = new Area(geometrySmaller);
		Area a1 = new Area(geometry);
		Area a2 = new Area(geometrySmaller);
		
		a1.intersect(a2);
		
		if(a1.equals(aPrev)) {
			
			return true;
		} else {
			return false;
		}
		
	}
	
	
	
	
	/**
	 * returns true if the geohash2 lies completely inside geohash1
	 * @author sapmitra
	 * @param geohash1 bigger geohash
	 * @param geohash2 smaller geohash
	 * @return
	 */
	public static boolean checkEnclosure(String geohash1, String geohash2) {
		
		if(geohash2.length() > geohash1.length()) {
			return false;
		}
		
		String g2_dash = geohash2.substring(0, geohash1.length());
		
		if(geohash1.equals(g2_dash)) {
			return true;
		} else {
			return false;
		}
		
		
	}
	
	
	/**
	 * check if two geohashes overlap
	 * @author sapmitra
	 * @param geohash1
	 * @param geohash2
	 * @return
	 */
	public static boolean checkIntersection(String geohash1, String geohash2) {
		
		SpatialRange range1 = decodeHash(geohash1);
		
		Coordinates c1 = new Coordinates(range1.getLowerBoundForLatitude(), range1.getLowerBoundForLongitude());
		Coordinates c2 = new Coordinates(range1.getUpperBoundForLatitude(), range1.getLowerBoundForLongitude());
		Coordinates c3 = new Coordinates(range1.getUpperBoundForLatitude(), range1.getUpperBoundForLongitude());
		Coordinates c4 = new Coordinates(range1.getLowerBoundForLatitude(), range1.getUpperBoundForLongitude());
		
		ArrayList<Coordinates> cs1 = new ArrayList<Coordinates>();
		cs1.add(c1);cs1.add(c2);cs1.add(c3);cs1.add(c4);
		
		
		SpatialRange range2 = decodeHash(geohash2);
		
		Coordinates c12 = new Coordinates(range2.getLowerBoundForLatitude(), range2.getLowerBoundForLongitude());
		Coordinates c22 = new Coordinates(range2.getUpperBoundForLatitude(), range2.getLowerBoundForLongitude());
		Coordinates c32 = new Coordinates(range2.getUpperBoundForLatitude(), range2.getUpperBoundForLongitude());
		Coordinates c42 = new Coordinates(range2.getLowerBoundForLatitude(), range2.getUpperBoundForLongitude());
		
		ArrayList<Coordinates> cs2 = new ArrayList<Coordinates>();
		cs2.add(c12);cs2.add(c22);cs2.add(c32);cs2.add(c42);
		
		return checkIntersection(cs1,cs2);
	}
	
	public static void main2(String arg[]) {
	
		Coordinates c11 = new Coordinates(50.18f, -103.29f);
		Coordinates c21 = new Coordinates(36.78f, -117.88f);
		Coordinates c31 = new Coordinates(36.85f, -97.21f);
		List<Coordinates> cl1 = new ArrayList<Coordinates>();
		cl1.add(c11);
		cl1.add(c21);
		cl1.add(c31);
		
		SpatialRange range = decodeHash("9xn");
		
		Coordinates c1 = new Coordinates(range.getLowerBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c2 = new Coordinates(range.getUpperBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c3 = new Coordinates(range.getUpperBoundForLatitude(), range.getUpperBoundForLongitude());
		Coordinates c4 = new Coordinates(range.getLowerBoundForLatitude(), range.getUpperBoundForLongitude());
		
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		
		//String[] intersectingGeohashes = getIntersectingGeohashes(cl, 6);
		System.out.println(checkIntersection(cl1,cl));
		/*for(String s: intersectingGeohashes) {
			if(s.startsWith("9x"))
				System.out.print(s+",");
		}*/
		
		//getSuperCubeGeohashBounds("sp", null, null, null, null, 3);
		
		//GeoHash.getBorderingGeoHashes("9r", 3, 1);
		
		
	}

	/**
	 * Decode GeoHash bits from a binary GeoHash.
	 *
	 * @param bits
	 *            ArrayList of Booleans containing the GeoHash bits
	 *
	 * @param latitude
	 *            If set to <code>true</code> the latitude bits are decoded. If
	 *            set to <code>false</code> the longitude bits are decoded.
	 *
	 * @return low, high range that the GeoHashed location falls between.
	 */
	private static float[] decodeBits(ArrayList<Boolean> bits, boolean latitude) {
		float low, high, middle;
		int offset;

		if (latitude) {
			offset = 1;
			low = -90.0f;
			high = 90.0f;
		} else {
			offset = 0;
			low = -180.0f;
			high = 180.0f;
		}

		for (int i = offset; i < bits.size(); i += 2) {
			middle = (high + low) / 2;

			if (bits.get(i)) {
				low = middle;
			} else {
				high = middle;
			}
		}

		if (latitude) {
			return new float[] { low, high };
		} else {
			return new float[] { low, high };
		}
	}

	/**
	 * Converts a GeoHash string to its binary representation.
	 *
	 * @param hash
	 *            GeoHash string to convert to binary
	 *
	 * @return The GeoHash in binary form, as an ArrayList of Booleans.
	 */
	private static ArrayList<Boolean> getBits(String hash) {
		hash = hash.toLowerCase();

		/* Create an array of bits, 5 bits per character: */
		ArrayList<Boolean> bits = new ArrayList<Boolean>(hash.length() * BITS_PER_CHAR);

		/* Loop through the hash string, setting appropriate bits. */
		for (int i = 0; i < hash.length(); ++i) {
			int charValue = charLookupTable.get(hash.charAt(i));

			/* Set bit from charValue, then shift over to the next bit. */
			for (int j = 0; j < BITS_PER_CHAR; ++j, charValue <<= 1) {
				bits.add((charValue & 0x10) == 0x10);
			}
		}
		return bits;
	}

	public static Polygon buildAwtPolygon(List<Coordinates> geometry) {
		Polygon polygon = new Polygon();
		for (Coordinates coords : geometry) {
			Point<Integer> point = coordinatesToXY(coords);
			polygon.addPoint(point.X(), point.Y());
		}
		return polygon;
	}

	/**
	 * 
	 * @param polygon
	 * @param gh
	 * @param bitPrecision geohash precision in bits instead of characters of the filesystem (fixed)
	 * @param intersections
	 */
	public static void getGeohashPrefixes(Polygon polygon, GeoHash gh, int bitPrecision, Set<GeoHash> intersections) {
		if (gh.getPrecision() >= bitPrecision) {
			intersections.add(gh);
		} else {
			if (polygon.contains(gh.getRectangle())) {
				intersections.add(gh);
			} else {
				GeoHash leftGH = new GeoHash(gh.getBinaryHash() + "0");
				GeoHash rightGH = new GeoHash(gh.getBinaryHash() + "1");
				if (polygon.intersects(leftGH.getRectangle()))
					getGeohashPrefixes(polygon, leftGH, bitPrecision, intersections);
				if (polygon.intersects(rightGH.getRectangle()))
					getGeohashPrefixes(polygon, rightGH, bitPrecision, intersections);
			}
		}
	}
	
	
	/**
	 * find out what the valid neighbors to a geohash are with respect to a query polygon
	 * @author sapmitra
	 * @param queryPolygon
	 * @param precision
	 * @param cGeoString
	 *  a------b
		.      .
		.      .
		d------c
	 * @param blocks 
	 * @param borderMap 
	 * @return
	 */
	public static String[] checkForNeighborValidity(List<Coordinates> polygon, int precision, String cGeoString, Map<String, BorderingProperties> borderMap, List<String> blocks ) {
		List<String> ignore = new ArrayList<String>();
		
		BorderingProperties bp = getBorderingGeohashHeuristic(cGeoString, precision, 0, null, null);
		
		String ne = bp.getNe();
			
		String nw = bp.getNw();
			
		String se = bp.getSe();
			
		String sw = bp.getSw();
		
		//System.out.println(nw);
		
		SpatialRange nwBounds = decodeHash(nw);
		SpatialRange neBounds = decodeHash(ne);
		SpatialRange seBounds = decodeHash(se);
		SpatialRange swBounds = decodeHash(sw);
		
		/* Getting North West Flank */
		Coordinates caNW = new Coordinates(nwBounds.getUpperBoundForLatitude(), nwBounds.getLowerBoundForLongitude());
		Coordinates cbNW = new Coordinates(nwBounds.getUpperBoundForLatitude(), nwBounds.getUpperBoundForLongitude());
		Coordinates ccNW = new Coordinates(nwBounds.getLowerBoundForLatitude(), nwBounds.getUpperBoundForLongitude());
		Coordinates cdNW = new Coordinates(nwBounds.getLowerBoundForLatitude(), nwBounds.getLowerBoundForLongitude());
		
		List<Coordinates> nwFlank = Arrays.asList(caNW, cbNW, ccNW, cdNW);
		
		//System.out.println(nwFlank);
		
		/* Getting North East Flank */
		Coordinates caNE = new Coordinates(neBounds.getUpperBoundForLatitude(), neBounds.getLowerBoundForLongitude());
		Coordinates cbNE = new Coordinates(neBounds.getUpperBoundForLatitude(), neBounds.getUpperBoundForLongitude());
		Coordinates ccNE = new Coordinates(neBounds.getLowerBoundForLatitude(), neBounds.getUpperBoundForLongitude());
		Coordinates cdNE = new Coordinates(neBounds.getLowerBoundForLatitude(), neBounds.getLowerBoundForLongitude());
		
		List<Coordinates> neFlank = Arrays.asList(caNE, cbNE, ccNE, cdNE);
		
		//System.out.println(neFlank);
		
		/* Getting South West Flank */
		Coordinates caSW = new Coordinates(swBounds.getUpperBoundForLatitude(), swBounds.getLowerBoundForLongitude());
		Coordinates cbSW = new Coordinates(swBounds.getUpperBoundForLatitude(), swBounds.getUpperBoundForLongitude());
		Coordinates ccSW = new Coordinates(swBounds.getLowerBoundForLatitude(), swBounds.getUpperBoundForLongitude());
		Coordinates cdSW = new Coordinates(swBounds.getLowerBoundForLatitude(), swBounds.getLowerBoundForLongitude());
		
		List<Coordinates> swFlank = Arrays.asList(caSW, cbSW, ccSW, cdSW);
		
		//System.out.println(swFlank);
		
		/* Getting South East Flank */
		Coordinates caSE = new Coordinates(seBounds.getUpperBoundForLatitude(), seBounds.getLowerBoundForLongitude());
		Coordinates cbSE = new Coordinates(seBounds.getUpperBoundForLatitude(), seBounds.getUpperBoundForLongitude());
		Coordinates ccSE = new Coordinates(seBounds.getLowerBoundForLatitude(), seBounds.getUpperBoundForLongitude());
		Coordinates cdSE = new Coordinates(seBounds.getLowerBoundForLatitude(), seBounds.getLowerBoundForLongitude());
		
		List<Coordinates> seFlank = Arrays.asList(caSE, cbSE, ccSE, cdSE);
		
		//System.out.println(seFlank);
		
		/* Getting North,South,East,West Flank*/
		
		List<Coordinates> northFlank = Arrays.asList(caNW, cbNE, ccNE, cdNW);
		List<Coordinates> southFlank = Arrays.asList(caSW, cbSE, ccSE, cdSW);
		List<Coordinates> eastFlank = Arrays.asList(caNE, cbNE, ccSE, cdSE);
		List<Coordinates> westFlank = Arrays.asList(caNW, cbNW, ccSW, cdSW);
		
		boolean checkN = checkIntersection(polygon, northFlank);
		boolean checkS = checkIntersection(polygon, southFlank);
		boolean checkE = checkIntersection(polygon, eastFlank);
		boolean checkW = checkIntersection(polygon, westFlank);
		boolean checkNE = checkIntersection(polygon, neFlank);
		boolean checkNW = checkIntersection(polygon, nwFlank);
		boolean checkSE = checkIntersection(polygon, seFlank);
		boolean checkSW = checkIntersection(polygon, swFlank);
		
		/* Checking for North */
		if(!checkN) {
			
			ignore.addAll(Arrays.asList("n","nw","ne"));
			
		} else if(!checkNE || !checkNW) {
			
			if(!checkNE) {
				ignore.add("ne");
			}
			if(!checkNW) {
				ignore.add("nw");
			}
		}
		
		
		/* Checking for South */
		if(!checkS) {
			
			ignore.addAll(Arrays.asList("s","sw","se"));
			
		} else if(!checkSE || !checkSW) {
			
			if(!checkSE) {
				ignore.add("se");
			}
			if(!checkSW) {
				ignore.add("sw");
			}
		}
		
		/* Checking for East */
		if(!checkE) {
			
			ignore.addAll(Arrays.asList("e","ne","se"));
			
		} else if(!checkSE || !checkNE) {
			
			if(!checkNE) {
				ignore.add("ne");
			}
			if(!checkSE) {
				ignore.add("se");
			}
		}
		
		/* Checking for West */
		if(!checkW) {
			
			ignore.addAll(Arrays.asList("w","nw","sw"));
			
		} else if(!checkSE || !checkNW) {
			
			if(!checkNW) {
				ignore.add("nw");
			}
			if(!checkSW) {
				ignore.add("sw");
			}
		}
		
		
		/* Looking at bordering properties */
		/* In bordering properties, during storage, N & nw are disjoint areas */
		
		List<String> ignoreBorder = new ArrayList<String>(Arrays.asList("n","s","e","w","ne","nw","se","sw"));
		
		for(String block : blocks) {
			
			if(ignoreBorder.size() > 0) {
				
				BorderingProperties bpr = borderMap.get(block);
				
				if(ignoreBorder.contains("n")) {
					if(bpr.getNorthEntries().size() > 0) {
						ignoreBorder.remove("n");
					}
				}
				if(ignoreBorder.contains("s")) {
					if(bpr.getSouthEntries().size() > 0) {
						ignoreBorder.remove("s");
					}
				}
				if(ignoreBorder.contains("e")) {
					if(bpr.getEastEntries().size() > 0) {
						ignoreBorder.remove("e");
					}
				}
				if(ignoreBorder.contains("w")) {
					if(bpr.getWestEntries().size() > 0) {
						ignoreBorder.remove("w");
					}
				}
				if(ignoreBorder.contains("ne")) {
					if(bpr.getNeEntries().size() > 0) {
						ignoreBorder.remove("ne");
					}
				}
				if(ignoreBorder.contains("nw")) {
					if(bpr.getNwEntries().size() > 0) {
						ignoreBorder.remove("nw");
					}
				}
				if(ignoreBorder.contains("se")) {
					if(bpr.getSeEntries().size() > 0) {
						ignoreBorder.remove("se");
					}
				}
				if(ignoreBorder.contains("sw")) {
					if(bpr.getSwEntries().size() > 0) {
						ignoreBorder.remove("sw");
					}
				}
			
			}
			
			
		}
		ignore.addAll(ignoreBorder);
		
		
		Set<String> ignoring = new HashSet<String>(ignore);
		
		
		List<String> valids = new ArrayList<String>(Arrays.asList("n","s","e","w","ne","nw","se","sw"));
		
		valids.removeAll(ignoring);
		
		String[] validArray = new String[valids.size()+1];
		validArray[0] = cGeoString;
		int count = 1;
		
		for(String s: valids) {
			validArray[count] = getNeighbour(cGeoString, s);
			
			count++;
		}
		
		return validArray;
	}
	
	
	
	
	/**
	 * 
	 * @author sapmitra
	 * @param geoHash
	 * @param precision
	 * @param timeLapse
	 * @param tp
	 * @return
	 */
	public static BorderingProperties getBorderingGeohashHeuristic(String geoHash, int precision, int timeLapse, TemporalProperties tp, TemporalType temporalType) {
		
		BorderingProperties bg = new BorderingProperties();
		
		if(tp != null) {
			
			long startTS = getStartTimeStamp(tp, temporalType);
			long endTS = getEndTimeStamp(tp, temporalType);
			
			
			bg.setUp1(endTS);
			bg.setUp2(endTS - timeLapse);
			
			bg.setDown1(startTS);
			bg.setDown2(startTS + timeLapse);
		}
		
		String[] northAddFat = {"b","c","f","g","u","v","y","z"};
		String[] southAddFat = {"0","1","4","5","h","j","n","p"};
		String[] westAddFat = {"b","8","2","0"};
		String[] eastAddFat = {"z","x","r","p"};
		
		
		String[] northAddSlim = {"p","r","x","z"};
		String[] southAddSlim = {"0","2","8","b"};
		String[] westAddSlim = {"p","n","j","h","5","4","1","0"};
		String[] eastAddSlim = {"z","y","v","u","g","f","c","b"};
		
		List<String> north = Arrays.asList(geoHash);
		List<String> south = Arrays.asList(geoHash);
		List<String> east = Arrays.asList(geoHash);
		List<String> west = Arrays.asList(geoHash);
		
		
		int currentLength = 0;
		int orientation = 0;
		
		if(geoHash != null && geoHash.length() > 0) {
			orientation = geoHash.length()%2;
			currentLength = geoHash.length() + 1;
			
		} else {
			return null;
		}
		
		while(currentLength <= precision) {
			
			if(orientation == 0) {
				List<String> northLocal = new ArrayList<String>();
				List<String> southLocal = new ArrayList<String>();
				List<String> eastLocal = new ArrayList<String>();
				List<String> westLocal = new ArrayList<String>();
				
				for(String s : north) {
					for(String sapp: northAddFat) {
						northLocal.add(s+sapp);
					}
				}
				
				for(String s : south) {
					for(String sapp: southAddFat) {
						southLocal.add(s+sapp);
					}
				}
				
				for(String s : east) {
					for(String sapp: eastAddFat) {
						eastLocal.add(s+sapp);
					}
				}
				
				for(String s : west) {
					for(String sapp: westAddFat) {
						westLocal.add(s+sapp);
					}
				}
				
				north = northLocal;
				south = southLocal;
				east = eastLocal;
				west = westLocal;
				orientation++;
				
			} else {
				List<String> northLocal = new ArrayList<String>();
				List<String> southLocal = new ArrayList<String>();
				List<String> eastLocal = new ArrayList<String>();
				List<String> westLocal = new ArrayList<String>();
				
				for(String s : north) {
					for(String sapp: northAddSlim) {
						northLocal.add(s+sapp);
					}
				}
				
				for(String s : south) {
					for(String sapp: southAddSlim) {
						southLocal.add(s+sapp);
					}
				}
				
				for(String s : east) {
					for(String sapp: eastAddSlim) {
						eastLocal.add(s+sapp);
					}
				}
				
				for(String s : west) {
					for(String sapp: westAddSlim) {
						westLocal.add(s+sapp);
					}
				}
				
				north = northLocal;
				south = southLocal;
				east = eastLocal;
				west = westLocal;
				orientation++;
				
				
			}
			orientation = currentLength % 2;
			currentLength++;
		}
		
		bg.setN(north);
		bg.setS(south);
		bg.setE(east);
		bg.setW(west);
		
		bg.setNw(north.get(0));
		bg.setSw(south.get(0));
		bg.setNe(north.get(north.size() - 1));
		bg.setSe(south.get(south.size() - 1));
		
		return bg;
	}
	
	
	public static void main11(String arg[]) {
		
		/*Date d = new Date();
		TemporalProperties tp = new TemporalProperties(d.getTime());
		BorderingProperties borderingGeohashHeuristic = getBorderingGeohashHeuristic("9", 3, 10, tp);
		System.out.println("Hi");*/
		
		Coordinates c1 = new Coordinates(45.79f, -114.16f);
		Coordinates c2 = new Coordinates(45.79f, -99.31f);
		Coordinates c3 = new Coordinates(38.03f, -99.31f);
		Coordinates c4 = new Coordinates(38.03f, -114.16f);
		//Coordinates c5 = new Coordinates(36.78f, -107.64f);
		
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		//cl.add(c5);
		
		String[] ss = GeoHash.checkForNeighborValidity(cl, 4, "9x", null, null);
		
		for(String s: ss) {
			System.out.println(s);
		}
		
		System.out.println(GeoHash.checkIntersection("c2", "c26"));
		
		String[] hashes = {"c2","c8","9r","9x","9q","9w","cb","9z","9y","c9"};
		String[] valids = {"c2","c8","9r","9x","9q","9w"};
		
		System.out.println("Filtered");
		String[] filterUnwantedGeohashes = GeoHash.filterUnwantedGeohashes(hashes, valids);
		
		for(String s: filterUnwantedGeohashes) {
			System.out.println(s);
		}
		
		
	}
	
	public static void main(String arg[]) throws ParseException {
		
		/*Coordinates c1 = new Coordinates(39.55f, -111.82f);
		Coordinates c2 = new Coordinates(38.87f, -112.74f);
		Coordinates c3 = new Coordinates(38.61f, -111.50f);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		
		GeoHash.generateOuterPolygon(cl, 6);*/
		/*BorderingProperties bp = GeoHash.getBorderingGeohashHeuristic("9x", 3, 0, null);
		System.out.println(bp.getN());
		System.out.println(bp.getS());
		System.out.println(bp.getE());
		System.out.println(bp.getW());*/
		
		
		/*List<List<Coordinates>> allInnerFlanks = GeoHash.getAllInnerFlanks("9x", 3);
		List<List<Coordinates>> allOuterFlanks = GeoHash.getAllOuterFlanks("9x", 3);
		
		System.out.println(allInnerFlanks);
		System.out.println(allOuterFlanks);*/
		//System.out.println(Arrays.toString(GeoHash.getNeighbours("9xe")));
		//System.out.println(checkEnclosure("9xu", "9xu"));
		System.out.println(Integer.parseInt("00"));
		TemporalProperties tp = new TemporalProperties(1417463510813l);
		System.out.println(getStartTimeStamp("2014","12","2","xx",TemporalType.DAY_OF_MONTH));
		System.out.println(getEndTimeStamp("2014","12","1","xx",TemporalType.DAY_OF_MONTH));
		
		
	}
	
	private static long getEndTimeStamp(TemporalProperties tp, TemporalType temporalType) {
		if(tp == null) {
			return -1;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		calendar.setTimeInMillis(tp.getEnd());
		
		switch (temporalType) {
			case HOUR_OF_DAY:
				calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			case DAY_OF_MONTH:
				calendar.set(Calendar.HOUR_OF_DAY, 23);
			    calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			case MONTH:
				int m = calendar.get(Calendar.MONTH) + 1;
				if(m == 1 || m == 3 || m == 5 || m == 7 || m == 8 || m == 10 || m == 12 )
					calendar.set(Calendar.DAY_OF_MONTH, 31);
				else if(m == 2) {
					int yr = calendar.get(Calendar.YEAR) ;
					if(yr % 4 == 0) 
						calendar.set(Calendar.DAY_OF_MONTH, 29);
					else
						calendar.set(Calendar.DAY_OF_MONTH, 28);
				} else 
					calendar.set(Calendar.DAY_OF_MONTH, 30);
				
				calendar.set(Calendar.HOUR_OF_DAY, 23);
			    calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			case YEAR:
				calendar.set(Calendar.MONTH, 11);
				calendar.set(Calendar.DAY_OF_MONTH, 31);
				calendar.set(Calendar.HOUR_OF_DAY, 23);
			    calendar.set(Calendar.MINUTE, 59);
			    calendar.set(Calendar.SECOND, 59);
			    calendar.set(Calendar.MILLISECOND, 999);
			    break;
			    
		}
		
		long offset = TemporalHash.TIMEZONE.getRawOffset() - TimeZone.getDefault().getRawOffset();
	    
	    //Date d1 = new Date(calendar.getTimeInMillis() + offset);
	    //System.out.println("CONVERTED:" + d1);
	    
	    //System.out.println(calendar.getTimeInMillis() + offset);
	    return calendar.getTimeInMillis() + offset;
	    
	    //return calendar.getTimeInMillis();
		
	}
	
	private static long getStartTimeStamp(TemporalProperties tp, TemporalType temporalType) {
		if(tp == null) {
			return -1;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		calendar.setTimeInMillis(tp.getStart());
		
		
		
		switch (temporalType) {
			case HOUR_OF_DAY:
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			case DAY_OF_MONTH:
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			case MONTH:
				calendar.set(Calendar.DAY_OF_MONTH, 1);
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			case YEAR:
				calendar.set(Calendar.MONTH, 0);
				calendar.set(Calendar.DAY_OF_MONTH, 1);
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
			    calendar.set(Calendar.SECOND, 0);
			    calendar.set(Calendar.MILLISECOND, 0);
			    break;
			    
		}
	    
		long offset = TemporalHash.TIMEZONE.getRawOffset() - TimeZone.getDefault().getRawOffset();
	    
	    //Date d1 = new Date(calendar.getTimeInMillis() + offset);
	    //System.out.println("CONVERTED:" + d1);
	    
	    //System.out.println(calendar.getTimeInMillis() + offset);
	    return calendar.getTimeInMillis() + offset;
	    //return calendar.getTimeInMillis();
		
	}
	
	/**
	 * @author sapmitra
	 * Getting Super Coordinates. THIS IS THE FINAL METHOD THAT GIVES THE SUPER BOUNDS FOR A GIVEN GEOHASH
	 * @param geoHash
	 * @param precision
	 */
	public static List<Coordinates> getSuperGeohashes(String geoHash, int precision) {
		
		String nwNeighbor = getNeighbour(geoHash, "nw");
		String nwCornerGeohash = getCornerGeohash(nwNeighbor, precision, "se");
		Coordinates nwPoint = getCornerPoint(nwCornerGeohash, "nw");
		
		
		String neNeighbor = getNeighbour(geoHash, "ne");
		String neCornerGeohash = getCornerGeohash(neNeighbor, precision, "sw");
		Coordinates nePoint = getCornerPoint(neCornerGeohash, "ne");
		
		String swNeighbor = getNeighbour(geoHash, "sw");
		String swCornerGeohash = getCornerGeohash(swNeighbor, precision, "ne");
		Coordinates swPoint = getCornerPoint(swCornerGeohash, "sw");
		
		String seNeighbor = getNeighbour(geoHash, "se");
		String seCornerGeohash = getCornerGeohash(seNeighbor, precision, "nw");
		Coordinates sePoint = getCornerPoint(seCornerGeohash, "se");
		
		System.out.println(nwCornerGeohash);
		System.out.println(neCornerGeohash);
		System.out.println(seCornerGeohash);
		System.out.println(swCornerGeohash);
		
		List<Coordinates> bounds = new ArrayList<Coordinates>();
		bounds.add(swPoint);
		bounds.add(nwPoint);
		bounds.add(nePoint);
		bounds.add(sePoint);
		
		System.out.println(bounds);
		
		return bounds;
		
		
	}
	
	public static String getCornerGeohash(String geoHash, int precision, String direction) {

		// ADD ACCOMMODATION FOR TIME

		BorderingProperties bp = getBorderingGeohashHeuristic(geoHash, precision, 0, null, null);
		
		if("ne".equals(direction)) {
			
			return bp.getNe();
			
		} else if("nw".equals(direction)) {
			
			return bp.getNw();
			
		} else if("se".equals(direction)) {
			
			return bp.getSe();
			
		} else if("sw".equals(direction)) {
			
			return bp.getSw();
			
		}
		
		
		return null;
	}
	
	/**
	 * @author sapmitra
	 * @param geoHash
	 * @param direction
	 * @return
	 */
	public static Coordinates getCornerPoint(String geoHash, String direction) {
		
		SpatialRange range = decodeHash(geoHash);
		
		Coordinates c = null;
		
		if("nw".equals(direction)) {
			
			c = new Coordinates(range.getUpperBoundForLatitude(), range.getLowerBoundForLongitude());
			
		} else if("ne".equals(direction)) {
			
			c = new Coordinates(range.getUpperBoundForLatitude(), range.getUpperBoundForLongitude());
			
		} else if("sw".equals(direction)) {
			
			c = new Coordinates(range.getLowerBoundForLatitude(), range.getLowerBoundForLongitude());
			
		} else if("se".equals(direction)) {
			
			c = new Coordinates(range.getLowerBoundForLatitude(), range.getUpperBoundForLongitude());
			
		}
		
		return c;
	}

	/**
	 * 
	 * @author sapmitra
	 * @param hashes
	 * @param validNeighbors
	 * @return
	 */
	public static String[] filterUnwantedGeohashes(String[] hashes, String[] validNeighbors) {
		// TODO Auto-generated method stub
		
		if(hashes == null || hashes.length == 0) 
			return hashes;
		
		List<String> filteredHashes = new ArrayList<String>();
		
		for(String hash : hashes) {
			
			boolean valid = false;
			
			for(String n : validNeighbors) {
				if(checkIntersection(n, hash)) {
					valid = true;
					break;
				}
			}
			
			if(valid) {
				filteredHashes.add(hash);
			}
			
		}
		return (filteredHashes.toArray(new String[filteredHashes.size()]));
	}
	
	
	/**
	 * Get all outer flanks of a given geohash
	 * @author sapmitra
	 * @param geoHash
	 * @param precision
	 * @return
	 * 
	 * a-----b
	 * .	 .
	 * .	 .
	 * c-----d
	 * 
	 * returned flanks are in the order nw,n,ne,w,e,sw,s,se
	 */
	public static List<List<Coordinates>> getAllInnerFlanks(String geoHash, int precision) {
		
		
		BorderingProperties bp = getBorderingGeohashHeuristic(geoHash, precision, 0, null, null);
		
		String ne = bp.getNe();
			
		String nw = bp.getNw();
			
		String se = bp.getSe();
			
		String sw = bp.getSw();
		
		//System.out.println(nw);
		
		SpatialRange nwBounds = decodeHash(nw);
		SpatialRange neBounds = decodeHash(ne);
		SpatialRange seBounds = decodeHash(se);
		SpatialRange swBounds = decodeHash(sw);
		
		
		Coordinates caNW = new Coordinates(nwBounds.getUpperBoundForLatitude(), nwBounds.getLowerBoundForLongitude());
		Coordinates cbNW = new Coordinates(nwBounds.getUpperBoundForLatitude(), nwBounds.getUpperBoundForLongitude());
		Coordinates ccNW = new Coordinates(nwBounds.getLowerBoundForLatitude(), nwBounds.getUpperBoundForLongitude());
		Coordinates cdNW = new Coordinates(nwBounds.getLowerBoundForLatitude(), nwBounds.getLowerBoundForLongitude());
		
		List<Coordinates> nwFlank = Arrays.asList(caNW, cbNW, ccNW, cdNW);
		
		
		Coordinates caNE = new Coordinates(neBounds.getUpperBoundForLatitude(), neBounds.getLowerBoundForLongitude());
		Coordinates cbNE = new Coordinates(neBounds.getUpperBoundForLatitude(), neBounds.getUpperBoundForLongitude());
		Coordinates ccNE = new Coordinates(neBounds.getLowerBoundForLatitude(), neBounds.getUpperBoundForLongitude());
		Coordinates cdNE = new Coordinates(neBounds.getLowerBoundForLatitude(), neBounds.getLowerBoundForLongitude());
		
		List<Coordinates> neFlank = Arrays.asList(caNE, cbNE, ccNE, cdNE);
		
		
		Coordinates caSW = new Coordinates(swBounds.getUpperBoundForLatitude(), swBounds.getLowerBoundForLongitude());
		Coordinates cbSW = new Coordinates(swBounds.getUpperBoundForLatitude(), swBounds.getUpperBoundForLongitude());
		Coordinates ccSW = new Coordinates(swBounds.getLowerBoundForLatitude(), swBounds.getUpperBoundForLongitude());
		Coordinates cdSW = new Coordinates(swBounds.getLowerBoundForLatitude(), swBounds.getLowerBoundForLongitude());
		
		List<Coordinates> swFlank = Arrays.asList(caSW, cbSW, ccSW, cdSW);
		
		Coordinates caSE = new Coordinates(seBounds.getUpperBoundForLatitude(), seBounds.getLowerBoundForLongitude());
		Coordinates cbSE = new Coordinates(seBounds.getUpperBoundForLatitude(), seBounds.getUpperBoundForLongitude());
		Coordinates ccSE = new Coordinates(seBounds.getLowerBoundForLatitude(), seBounds.getUpperBoundForLongitude());
		Coordinates cdSE = new Coordinates(seBounds.getLowerBoundForLatitude(), seBounds.getLowerBoundForLongitude());
		
		List<Coordinates> seFlank = Arrays.asList(caSE, cbSE, ccSE, cdSE);
		
		
		List<Coordinates> northFlank = Arrays.asList(caNW, cbNE, ccNE, cdNW);
		List<Coordinates> southFlank = Arrays.asList(caSW, cbSE, ccSE, cdSW);
		List<Coordinates> eastFlank = Arrays.asList(caNE, cbNE, ccSE, cdSE);
		List<Coordinates> westFlank = Arrays.asList(caNW, cbNW, ccSW, cdSW);
		
		
		List<Coordinates> innerFlank = Arrays.asList(ccNW, cdNE, caSE, cbSW);
		
		List<List<Coordinates>> flanks = Arrays.asList(nwFlank,northFlank,neFlank,westFlank,innerFlank,eastFlank,swFlank,southFlank,seFlank);
		
		
		return flanks;
		
		
	}
	
	
	/**
	 * finding what part of geohash2 is actually needed
	 * Only that part will be loaded for a superpolygon and feature query
	 * @author sapmitra
	 * @param geoHash1 metadata geohash from fs1
	 * @param geoHash2 metadata geohash from fs2
	 * @param time1 metadata time from fs1
	 * @param time2 metadata time from fs2
	 * @return
	 * @throws ParseException 
	 */
	
	public static String getOrientation(String geoHash1, String geoHash2, String time1, String time2, TemporalType t1, TemporalType t2) throws ParseException {
		String temporalOrientation = getTemporalOrientation(time1,time2,t1,t2);
		if(temporalOrientation.contains("ignore"))
			return "ignore-ignore";
		String spatialOrientation = getSpatialOrientationHeuristic(geoHash1, geoHash2);
		
		return spatialOrientation+"-"+temporalOrientation;
		
	}
	
	
	private static String getTemporalOrientation(String time1, String time2, TemporalType t1, TemporalType t2) throws ParseException {
		
		String[] tokens = time1.split("-");
		String[] tokens2 = time2.split("-");
		
		long startTime1 = getStartTimeStamp(tokens[0], tokens[1], tokens[2], tokens[3], t1);
		long endTime1 = getEndTimeStamp(tokens[0], tokens[1], tokens[2], tokens[3], t1);
		
		long startTime2 = getStartTimeStamp(tokens2[0], tokens2[1], tokens2[2], tokens2[3], t2);
		long endTime2 = getEndTimeStamp(tokens2[0], tokens2[1], tokens2[2], tokens2[3], t2);
		
		// time1 encloses time2
		if(startTime2>=startTime1 && endTime2<=endTime1)
			return "full";
		// time2 encloses time1
		else if(startTime1>=startTime2 && endTime1<=endTime2)
			return "full";
		// time2 lies before time1
		else if(startTime1 == endTime2+1 && endTime2 < endTime1)
			return "down";
		// time2 lies after time1
		else if(endTime1+1 == startTime2 && startTime1 < startTime2)
			return "up";
		
		return "ignore";
	}
	
	/**
	 * 
	 * @author sapmitra
	 * @param g1
	 * @param g2
	 * @param p1
	 * @param p2
	 * @return
	 */
	public static String getSpatialOrientationHeuristic(String g1, String g2) {
		
		// g1 is greater than  equals in size than g2
		if(g1.length() <= g2.length()) {
			
			boolean enc = checkEnclosure(g1, g2);
			
			if(enc) {
				return "full";
			} else {
				
				// g2 may lie somewhere on the outskirts of g1
				
				//g2_dash is the bigger geohash that encloses g2, with the same size as g1
				String g2_dash =  g2.substring(0,g1.length());
				
				// Neighbors are returned in the order nw,n,ne,w,e,sw,s,se
				List<String> neighbors = new ArrayList<String>(Arrays.asList(getNeighbours(g1)));
				
				// g2 has to lie on the border of g1
				if(neighbors.contains(g2_dash)) {
					
					// What kind of neighbor is g2_dash
					int indx = neighbors.indexOf(g2_dash);
					
					BorderingProperties bp = getBorderingGeohashHeuristic(g2_dash, g2.length(), 0, null, null);
					
					return getChunkDirection(g2, indx, bp, 0);
					
					
				} else {
					return "ignore";
				}
				
			}
			
		} else if(g1.length() > g2.length()) {
			
			// g1 is smaller in size than g2
			boolean enc = checkEnclosure(g2, g1);
			
			if(enc) {
				//check inside
				BorderingProperties bp = getBorderingGeohashHeuristic(g2, g1.length(), 0, null, null);
				
				
				return "full";
				
				
			} else {
				
				// check outside
				String g1_dash =  g1.substring(0,g2.length());
				// Neighbors are returned in the order nw,n,ne,w,e,sw,s,se
				List<String> neighbors = new ArrayList<String>(Arrays.asList(getNeighbours(g2)));
				
				if(neighbors.contains(g1_dash)) {
					
					// whar kind of neighbbor is 
					int indx = neighbors.indexOf(g1_dash);
					
					BorderingProperties bp = getBorderingGeohashHeuristic(g1_dash, g1.length(), 0, null, null);
					
					return getChunkDirection(g1, indx, bp, 1);
					
				} else {
					return "ignore";
				}
				
			}
			
		}
		
		return "ignore";
		
	}

	/**
	 * Determines which flank of a geohash is needed
	 * @author sapmitra
	 * @param g2
	 * @param indx
	 * @param bp
	 * @return
	 */
	private static String getChunkDirection(String g2, int indx, BorderingProperties bp, int type) {
		// if g2_dash is a nw neighbor
		if(indx == 0) {
			
			// Only case where g2 would matter is if g2 is the se corner
			if(bp.getSe().equals(g2)) {
				
				if(type == 0)
					return "se";
				else if(type == 1)
					return "nw";
				
			}
			
		}
		// if g2_dash is a n neighbor
		else if(indx == 1) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getS().contains(g2)) {
				if(type == 0)
					return "s";
				else if(type == 1)
					return "n";
			}
		}
		
		// if g2_dash is a ne neighbor
		else if(indx == 2) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getSw().equals(g2)) {
				if(type == 0)
					return "sw";
				else if(type == 1)
					return "ne";
			}
		}
		
		// if g2_dash is a w neighbor
		else if(indx == 3) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getE().contains(g2)) {
				if(type == 0)
					return "e";
				else if(type == 1)
					return "w";
				
			}
		}
		
		// if g2_dash is a e neighbor
		else if(indx == 4) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getW().contains(g2)) {
				if(type == 0)
					return "w";
				else if(type == 1)
					return "e";
				
			}
		}
		// if g2_dash is a sw neighbor
		else if(indx == 5) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getNe().equals(g2)) {
				
				if(type == 0)
					return "ne";
				else if(type == 1)
					return "sw";
				
			}
		}
		
		// if g2_dash is a n neighbor
		else if(indx == 6) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getN().contains(g2)) {
				
				if(type == 0)
					return "n";
				else if(type == 1)
					return "s";
			}
		}
		
		// if g2_dash is a ne neighbor
		else if(indx == 7) {
			
			// Only case where g2 would matter is if g2 is the s corner
			if(bp.getNw().equals(g2)) {
				
				if(type == 0)
					return "nw";
				else if(type == 1)
					return "se";
			}
		} 
		return "ignore";
	}
	
}



