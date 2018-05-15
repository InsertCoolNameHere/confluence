/*
Copyright (c) 2014, Colorado State University
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

package galileo.bmp;

import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.dataset.Coordinates;
import galileo.dataset.Point;

/**
 * Provides functionality for transforming {@link GeoavailabilityQuery}
 * instances into Bitmaps that can be used with a {@link GeoavailabilityGrid}.
 *
 * @author malensek
 */
public class QueryTransform {

	protected static final Logger logger = Logger.getLogger("galileo");

	public static Bitmap queryToGridBitmap(GeoavailabilityQuery query, GeoavailabilityGrid grid) {

		/* Convert lat, lon coordinates into x, y points on the grid */
		List<Coordinates> poly = query.getPolygon();
		Polygon p = new Polygon();
		for (Coordinates coords : poly) {
			Point<Integer> point = grid.coordinatesToXY(coords);
			p.addPoint(point.X(), point.Y());
		}

		Area polygonArea = new Area(p);
		Area gridArea = new Area(new Rectangle(0, 0, grid.getWidth(), grid.getHeight()));
		/*
		 * compute the intersection region and use that region as the polygon to
		 * build the query bitmap
		 */
		
		/* Puro grid e polygon ta kotota area cover koreche */
		polygonArea.intersect(gridArea);
		
		/*if(polygonArea.isEmpty()) {
			logger.info("RIKI: MY CHANGE, QUERYBITMAP SET TO NULL");
			return null;
		}*/

		/*
		 * Determine the minimum bounding rectangle (MBR) of the polygon.
		 * Rectangle boundingBox = p.getBounds();
		 */
		/*
		 * Ignoring the orignial polygon and treating the geometry outline of
		 * the intersecting region between the original polygon and the grid as
		 * the new polygon
		 */
		Rectangle boundingBox = polygonArea.getBounds();
		int x = (int) boundingBox.getX();
		int y = (int) boundingBox.getY();
		int w = (int) boundingBox.getWidth();
		int h = (int) boundingBox.getHeight();

		/*
		 * if bounding box starts with negative x, we will map it on the grid
		 * starting at 0, so shift is not needed.
		 * 
		 * Removing the upper 2 5bit things
		 */
		int shift = (x > 0) ? x % 64 : 0;
		w = w + shift;
		p = new Polygon();
		for (Coordinates coords : poly) {
			Point<Integer> point = grid.coordinatesToXY(coords);
			p.addPoint(point.X() + shift, point.Y());
		}

		/*
		 * Calculate shift factors. This method outputs Bitmaps that are
		 * word-aligned (64 bit boundaries). If the extra width or height added
		 * overflows, then the shift factors are adjusted accordingly.
		 */
		int wshift = (64 - (w % 64)) % 64;
		int hshift = (64 - (h % 64)) % 64;

		w = w + wshift;
		h = h + hshift;

		/*if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, "Converting query polygon to " + "GeoavailabilityGrid bitmap. {0}x{1} at ({2}, {3});"
					+ " shifts: +({4}, {5})", new Object[] { w, h, x, y, wshift, hshift });
			logger.log(Level.INFO, "Polygon shifted by {0} along x axis on the new plane", shift);
		}*/

		BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_INDEXED);
		Graphics2D g = img.createGraphics();
		/* Apply these x, y transformations to the resulting image */
		AffineTransform transform = new AffineTransform();
		transform.translate(-x, -y);
		g.setTransform(transform);

		g.fillPolygon(p);
		g.dispose();

		/* Convert the bytes into a Bitmap representation */
		/* Get the raw image data, in bytes */
		DataBufferByte buffer = ((DataBufferByte) img.getData().getDataBuffer());
		byte[] data = buffer.getData();
		Bitmap queryBitmap = Bitmap.fromBytes(data, x, y, w, h, grid.getWidth(), grid.getHeight());
		return queryBitmap;
	}
	
	public static Bitmap queryToGridBitmapTest(List<Coordinates> poly, GeoavailabilityGrid grid, Graphics2D gr) {

		/* Convert lat, lon coordinates into x, y points on the grid */
		Polygon p = new Polygon();
		
		for (Coordinates coords : poly) {
			Point<Integer> point = grid.coordinatesToXY(coords);
			p.addPoint(point.X(), point.Y());
		}

		Area polygonArea = new Area(p);
		Area gridArea = new Area(new Rectangle(0, 0, grid.getWidth(), grid.getHeight()));
		/*
		 * compute the intersection region and use that region as the polygon to
		 * build the query bitmap
		 */
		gr.draw(p);
		
		
		polygonArea.intersect(gridArea);

		/*
		 * Determine the minimum bounding rectangle (MBR) of the polygon.
		 * Rectangle boundingBox = p.getBounds();
		 */
		/*
		 * Ignoring the orignial polygon and treating the geometry outline of
		 * the intersecting region between the original polygon and the grid as
		 * the new polygon
		 */
		Rectangle boundingBox = polygonArea.getBounds();
		int x = (int) boundingBox.getX();
		int y = (int) boundingBox.getY();
		int w = (int) boundingBox.getWidth();
		int h = (int) boundingBox.getHeight();
		
		gr.fill(boundingBox);
		/*
		 * if bounding box starts with negative x, we will map it on the grid
		 * starting at 0, so shift is not needed.
		 */
		int shift = (x > 0) ? x % 64 : 0;
		w = w + shift;
		p = new Polygon();
		for (Coordinates coords : poly) {
			Point<Integer> point = grid.coordinatesToXY(coords);
			p.addPoint(point.X() + shift, point.Y());
		}

		/*
		 * Calculate shift factors. This method outputs Bitmaps that are
		 * word-aligned (64 bit boundaries). If the extra width or height added
		 * overflows, then the shift factors are adjusted accordingly.
		 */
		int wshift = (64 - (w % 64)) % 64;
		int hshift = (64 - (h % 64)) % 64;

		w = w + wshift;
		h = h + hshift;

		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, "Converting query polygon to " + "GeoavailabilityGrid bitmap. {0}x{1} at ({2}, {3});"
					+ " shifts: +({4}, {5})", new Object[] { w, h, x, y, wshift, hshift });
			logger.log(Level.INFO, "Polygon shifted by {0} along x axis on the new plane", shift);
		}

		BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_INDEXED);
		Graphics2D g = img.createGraphics();
		/* Apply these x, y transformations to the resulting image */
		AffineTransform transform = new AffineTransform();
		transform.translate(-x, -y);
		g.setTransform(transform);

		g.fillPolygon(p);
		g.dispose();

		/* Convert the bytes into a Bitmap representation */
		/* Get the raw image data, in bytes */
		DataBufferByte buffer = ((DataBufferByte) img.getData().getDataBuffer());
		byte[] data = buffer.getData();
		Bitmap queryBitmap = Bitmap.fromBytes(data, x, y, w, h, grid.getWidth(), grid.getHeight());
		return queryBitmap;
	}
}
