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

import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import galileo.dataset.Coordinates;
import galileo.dataset.Point;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dht.Partitioner;
import galileo.dht.hash.TemporalHash;
import galileo.fs.GeospatialFileSystem;

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
	
	/**
	 * 
	 * @author sapmitra
	 * @param precision
	 * @param binHash
	 * @return
	 */
	public static String[] getGeoHashValues(int precision, String binHash) {
		String[] values = null;
		String hash = "";
		for (int i = 0; i < binHash.length(); i += 5) {
			String hashChar = binHash.substring(i, java.lang.Math.min(i + 5, binHash.length()));
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
	
	public static void Xmain(String arg[]) {
		
		SpatialRange range = decodeHash("f6");
		
		Coordinates c1 = new Coordinates(range.getLowerBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c2 = new Coordinates(range.getUpperBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c3 = new Coordinates(range.getUpperBoundForLatitude(), range.getUpperBoundForLongitude());
		Coordinates c4 = new Coordinates(range.getLowerBoundForLatitude(), range.getUpperBoundForLongitude());
		
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		
		System.out.println(GeoHash.encode(range, 2));
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		
		String[] intersectingGeohashesForConvexBoundingPolygon = GeoHash.getIntersectingGeohashesForConvexBoundingPolygon(cl,3);
	
		System.out.println(intersectingGeohashesForConvexBoundingPolygon);
		
		System.out.println(1<<30);
		
		int width = 1 << MAX_PRECISION;
		System.out.println(width);
		
		Point<Integer> coordinatesToXY = GeoHash.coordinatesToXY(c1);
		
		System.out.println("Hi");
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
	 * @author sapmitra
	 * @param geoHash
	 * @param precision
	 * @return
	 */
	public static String[] getInternalGeohashes(String geoHash, int precision) {
		
		SpatialRange range = decodeHash(geoHash);
		Coordinates c1 = new Coordinates(range.getLowerBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c2 = new Coordinates(range.getUpperBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c3 = new Coordinates(range.getUpperBoundForLatitude(), range.getUpperBoundForLongitude());
		Coordinates c4 = new Coordinates(range.getLowerBoundForLatitude(), range.getUpperBoundForLongitude());
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		
		String[] intersectingGeohashes = GeoHash.getIntersectingGeohashesForConvexBoundingPolygon(cl, precision);
		
		//System.out.println(intersectingGeohashes.length);
		
		/*for(int i=0;i<intersectingGeohashes.length;i++) {
			System.out.print(intersectingGeohashes[i] +" ");
		}*/
		return intersectingGeohashes;
	}
	
	public static void getBorderGeoHashes(String geoHash, int precision) {
		
		String[] internalGeohashes = getInternalGeohashes(geoHash, precision);
		
		List<String> internalGeohashesList = Arrays.asList(internalGeohashes);
		
		int count = 0;
		for(String geo: internalGeohashesList) {
			String[] nei = getNeighbours(geo);
			
			List<String> neighborsList = Arrays.asList(nei);
			
			if(!internalGeohashesList.containsAll(neighborsList)) {
				System.out.println(geo);
				count++;
			}
			
			
		}
		System.out.println(count);
		
	}
	
	
	private static long getEndTimeStamp(TemporalProperties tp) {
		if(tp == null) {
			return 0;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		calendar.setTimeInMillis(tp.getEnd());
		
		calendar.set(Calendar.HOUR_OF_DAY, 23);
	    calendar.set(Calendar.MINUTE, 59);
	    calendar.set(Calendar.SECOND, 59);
	    calendar.set(Calendar.MILLISECOND, 999);
	    
	    return calendar.getTimeInMillis();
		
	}
	
	
	private static long getStartTimeStamp(TemporalProperties tp) {
		if(tp == null) {
			return 0;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		calendar.setTimeInMillis(tp.getStart());
		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
	    calendar.set(Calendar.MINUTE, 0);
	    calendar.set(Calendar.SECOND, 0);
	    calendar.set(Calendar.MILLISECOND, 0);
	    
	    return calendar.getTimeInMillis();
		
	}
	
	
	
	/* Get NSEW Geohashes. USELESS. */
	
	public static BorderingProperties getBorderingGeoHashes(String geoHash, int precision, int timeLapse, TemporalProperties tp) {
		
		// ADD ACCOMMODATION FOR TIME
		long startTS = getStartTimeStamp(tp);
		long endTS = getEndTimeStamp(tp);
		
		String[] internalGeohashes = getInternalGeohashes(geoHash, precision);
		
		List<String> internalGeohashesList = Arrays.asList(internalGeohashes);
		
		BorderingProperties bg = new BorderingProperties();
		
		bg.setUp1(endTS);
		bg.setUp2(endTS - timeLapse);
		
		bg.setDown1(startTS);
		bg.setDown2(startTS + timeLapse);
		
		
		
		for(String geo: internalGeohashesList) {
			//String[] nei = getNeighbours(geo);
			
			String nn = getNeighbour(geo, "n");
			String sn = getNeighbour(geo, "s");
			String en = getNeighbour(geo, "e");
			String wn = getNeighbour(geo, "w");
			
			String nen = getNeighbour(geo, "ne");
			String nwn = getNeighbour(geo, "nw");
			String swn = getNeighbour(geo, "sw");
			String sen = getNeighbour(geo, "se");
			
			
			if( !internalGeohashesList.contains(nn) && !internalGeohashesList.contains(nwn) && !internalGeohashesList.contains(wn) ) {
				
				bg.setNw(geo);
				
			} else if( !internalGeohashesList.contains(nn) && !internalGeohashesList.contains(nen) && !internalGeohashesList.contains(en) ) {
				
				bg.setNe(geo);
				
			} else if( !internalGeohashesList.contains(sn) && !internalGeohashesList.contains(sen) && !internalGeohashesList.contains(en) ) {
				
				bg.setSe(geo);
				
			} else if( !internalGeohashesList.contains(sn) && !internalGeohashesList.contains(swn) && !internalGeohashesList.contains(wn) ) {
				
				bg.setSw(geo);
				
			} else if( !internalGeohashesList.contains(sn) ) {
				
				bg.addS(geo);
				
			} else if( !internalGeohashesList.contains(nn) ) {
				
				bg.addN(geo);
				
			} else if( !internalGeohashesList.contains(en) ) {
				
				bg.addE(geo);
				
			} else if( !internalGeohashesList.contains(wn) ) {
				
				bg.addW(geo);
				
			}	
			
		}
		
		printBorders(bg);
		return bg;
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
	
	/**
	 * Input is a geohash and get a particular corner geohash
	 * @author sapmitra
	 * @param geoHash
	 * @param precision
	 * @param direction
	 * @return
	 */
	public static String getCornerGeohash(String geoHash, int precision, String direction) {

		// ADD ACCOMMODATION FOR TIME

		String[] internalGeohashes = getInternalGeohashes(geoHash, precision);

		List<String> internalGeohashesList = Arrays.asList(internalGeohashes);

		BorderingProperties bg = new BorderingProperties();
		String nwGeo = "",swGeo="",neGeo="",seGeo="";

		for (String geo : internalGeohashesList) {
			String[] nei = getNeighbours(geo);

			String nn = getNeighbour(geo, "n");
			String sn = getNeighbour(geo, "s");
			String en = getNeighbour(geo, "e");
			String wn = getNeighbour(geo, "w");

			String nen = getNeighbour(geo, "ne");
			String nwn = getNeighbour(geo, "nw");
			String swn = getNeighbour(geo, "sw");
			String sen = getNeighbour(geo, "se");
			

			if (!internalGeohashesList.contains(nn) && !internalGeohashesList.contains(nwn)
					&& !internalGeohashesList.contains(wn)) {

				nwGeo = geo;

			} else if (!internalGeohashesList.contains(nn) && !internalGeohashesList.contains(nen)
					&& !internalGeohashesList.contains(en)) {

				neGeo = geo;

			} else if (!internalGeohashesList.contains(sn) && !internalGeohashesList.contains(sen)
					&& !internalGeohashesList.contains(en)) {

				seGeo = geo;

			} else if (!internalGeohashesList.contains(sn) && !internalGeohashesList.contains(swn)
					&& !internalGeohashesList.contains(wn)) {

				swGeo = geo;

			}

		}
		
		
		if("ne".equals(direction)) {
			
			return neGeo;
			
		} else if("nw".equals(direction)) {
			
			return nwGeo;
			
		} else if("se".equals(direction)) {
			
			return seGeo;
			
		} else if("sw".equals(direction)) {
			
			return swGeo;
			
		}
		
		
		

		return "";
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
		
		if("ne".equals(direction)) {
			
			c = new Coordinates(range.getUpperBoundForLatitude(), range.getUpperBoundForLongitude());
			
		} else if("nw".equals(direction)) {
			
			c = new Coordinates(range.getUpperBoundForLatitude(), range.getLowerBoundForLongitude());
			
		} else if("se".equals(direction)) {
			
			c = new Coordinates(range.getLowerBoundForLatitude(), range.getUpperBoundForLongitude());
			
		} else if("sw".equals(direction)) {
			
			c = new Coordinates(range.getLowerBoundForLatitude(), range.getLowerBoundForLongitude());
			
		}
		
		return c;
	}
	
	private static void printBorders(BorderingProperties bg) {
		
		System.out.println("NE: "+bg.getNe());
		System.out.println("NW: "+bg.getNw());
		System.out.println("SE: "+bg.getSe());
		System.out.println("NW: "+bg.getSw());
		
		System.out.print("N: ");
		
		for(String s: bg.getN()) {
			System.out.print(s+" ");
		}
		System.out.println();
		
		System.out.print("S: ");
		
		for(String s: bg.getS()) {
			System.out.print(s+" ");
		}
		System.out.println();
		
		System.out.print("E: ");
		
		for(String s: bg.getE()) {
			System.out.print(s+" ");
		}
		System.out.println();
		
		System.out.print("W: ");
		
		for(String s: bg.getW()) {
			System.out.print(s+" ");
		}
		System.out.println();
		
	}

	public static void main(String arg[]) {
		
		SpatialRange range = decodeHash("sp");
		Coordinates c1 = new Coordinates(range.getLowerBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c2 = new Coordinates(range.getUpperBoundForLatitude(), range.getLowerBoundForLongitude());
		Coordinates c3 = new Coordinates(range.getUpperBoundForLatitude(), range.getUpperBoundForLongitude());
		Coordinates c4 = new Coordinates(range.getLowerBoundForLatitude(), range.getUpperBoundForLongitude());
		
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		
		getSuperGeohashes("sp", 3);
		
		//getSuperCubeGeohashBounds("sp", null, null, null, null, 3);
		
		//GeoHash.getBorderingGeoHashes("9r", 3, 1);
		
		
	}
	
	/*public static void Xmain(String arg[]) {
		
		
		Coordinates c1 = new Coordinates(40.68f, -127.86f);
		Coordinates c2 = new Coordinates(38.68f, -95.86f);
		Coordinates c3 = new Coordinates(8.68f, -97.86f);
		Coordinates c4 = new Coordinates(9.68f, -129.86f);
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		
		Polygon  p = GeoHash.buildAwtPolygon(cl);
		
		GeoHash gh = new GeoHash();
		
		Set<GeoHash> ss = new HashSet<GeoHash>();
		
		GeoHash.getGeohashPrefixes(p, gh, 4*5, ss);
		
		for(GeoHash iGh : ss)
			System.out.println(Arrays.toString(iGh.getValues(4)));
	}*/
}
