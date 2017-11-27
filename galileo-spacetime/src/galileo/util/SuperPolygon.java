package galileo.util;
import java.util.ArrayList;
import java.util.List;

import galileo.dataset.Coordinates;
import galileo.dataset.SpatialRange;

/**
 * 
 * @author sapmitra
 *
 */
public class SuperPolygon {
	
	
	public static List<Coordinates> getSuperPolygon(List<Coordinates> polygon, int precision) {
		
		String geohash = GeoHash.encode(polygon.get(0), precision);
		
		SpatialRange boundingBox = GeoHash.decodeHash(geohash);
		
		float widthDiff = boundingBox.getUpperBoundForLatitude() - boundingBox.getLowerBoundForLatitude();
		float heightDiff = boundingBox.getLowerBoundForLongitude() - boundingBox.getUpperBoundForLongitude();
		
		float d = (float)(java.lang.Math.sqrt(widthDiff*widthDiff + heightDiff*heightDiff));
		
		int size = polygon.size();
		List<Coordinates> updatedPolygon = new ArrayList<Coordinates>();
		
		
		for(int i=0; i < polygon.size() ;i++) {
			
			Coordinates c1 = polygon.get(i);
			Coordinates c2;
			if(i+1 < size) {
				c2 = polygon.get(i+1);
			} else {
				c2 = polygon.get((i+1)%size);
			}
			Coordinates c3;
			
			if(i+2 < size) {
				c3 = polygon.get(i+2);
			} else {
				c3 = polygon.get((i+2)%size);
			}
			
			Coordinates cin = calculateIncenter(c1,c2,c3);
			 
			// Get line equation
			// Get points at +- d from c2
			// check which point increases the polygon area
			// update the polygon
			// https://math.stackexchange.com/questions/409689/how-do-i-find-a-point-a-given-distance-from-another-point-along-a-line
			// Calculate area of a polygon : https://www.wikihow.com/Calculate-the-Area-of-a-Polygon
			
			updatedPolygon.add(getExternalPoint(c2, cin, d, polygon));
		}
		
		return updatedPolygon;
		
		
	}



	/**
	 * @param c2
	 * @param cin
	 * @param d
	 * @param polygon
	 * @return
	 */
	private static Coordinates getExternalPoint(Coordinates c2, Coordinates cin, float d, List<Coordinates> polygon) {
		float d_dash = getSide(c2,cin);
		
		float x = c2.getLatitude();
		float y = c2.getLongitude();
		
		float x1_1 = x + (d/d_dash)*(x - cin.getLatitude());
		float y1_1 = y + (d/d_dash)*(y - cin.getLongitude());
		
		float x1_2 = x + ((d*-1)/d_dash)*(x - cin.getLatitude());
		float y1_2 = y + ((d*-1)/d_dash)*(y - cin.getLongitude());
		
		Coordinates opt1 = new Coordinates(x1_1, y1_1);
		Coordinates opt2 = new Coordinates(x1_2, y1_2);
		
		Coordinates finalCoordinate = getFatterPolygon(polygon, opt1, opt2, c2);
		
		
		return finalCoordinate;
	}



	/**
	 * @param polygon
	 * @param opt1
	 * @param opt2
	 * @param c2
	 * @return
	 */
	private static Coordinates getFatterPolygon(List<Coordinates> polygon, Coordinates opt1, Coordinates opt2, Coordinates c2) {
		
		int indx = polygon.indexOf(c2);
		
		List<Coordinates> polygon1 = cloneCoordinates(polygon, indx, opt1);
		List<Coordinates> polygon2 = cloneCoordinates(polygon, indx, opt2);
		
		float area1 = calculatePolygonArea(polygon1);
		float area2 = calculatePolygonArea(polygon2);
		
		if(area1 > area2 ) {
			return opt1;
		} else {
			return opt2;
		}
		
	}
	
	private static List<Coordinates> cloneCoordinates(List<Coordinates> polygon, int indx, Coordinates cnew) {
		
		List<Coordinates> polygonNew = new ArrayList<Coordinates>();
		int count = 0;
		
		for(Coordinates c: polygon ) {
			
			if(count != indx) {
				Coordinates c1 = new Coordinates(c.getLatitude(), c.getLongitude());
				polygonNew.add(c1);
			} else {
				polygonNew.add(cnew);
			}
			count++;
		}
		
		return polygonNew;
	}
	
	private static float calculatePolygonArea(List<Coordinates> polygon) {
	 
		float sigmaLeft = 0;
		float sigmaRight = 0;
		
		for(int i = 0; i < polygon.size() - 1; i++) {
			
			Coordinates c1 = polygon.get(i);
			Coordinates c2 = polygon.get(i+1);
			
			sigmaLeft += c1.getLatitude()*c2.getLongitude();
			sigmaRight += c1.getLongitude()*c2.getLatitude();
			
		}
		
		float area = (float)(java.lang.Math.abs((sigmaLeft - sigmaRight)/2.0f)); 
		return area;
		
	}
	
	public static void main(String arg[]) {
		
		Coordinates c1 = new Coordinates(0, 0);
		Coordinates c2 = new Coordinates(5, 0);
		Coordinates c3 = new Coordinates(5, 5);
		
		List<Coordinates> l = new ArrayList<Coordinates>();
		l.add(c1);l.add(c2);l.add(c3);
		
		System.out.println(SuperPolygon.getSuperPolygon(l, 1));
	}



	/**
	 * @param A A
	 * @param B B
	 * @param C C
	 */
	private static Coordinates calculateIncenter(Coordinates A, Coordinates B, Coordinates C) {
		
		float a = getSide(B,C);
		float b = getSide(A,C);
		float c = getSide(B,A);
		
		float p = a + b + c;
		
		float inx = (a*A.getLatitude() + b*B.getLatitude() + c*C.getLatitude())/p;
		float iny = (a*A.getLongitude() + b*B.getLongitude() + c*C.getLongitude())/p;
		
		return new Coordinates(inx, iny);
		
	}
	
	
	private static float getSide(Coordinates c1, Coordinates c2) {
		
		float xdist = c1.getLatitude() - c2.getLatitude();
		float ydist = c1.getLongitude() - c2.getLongitude();
		
		float dist = (float)(java.lang.Math.sqrt(xdist*xdist + ydist*ydist));
		
		return dist;
	}

}
