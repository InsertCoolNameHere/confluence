// GraphicsTest
// Author: Saptashwa Mitra
// Date:   Oct 30, 2017
// Class:  CS163
// Email:  eid@cs.colostate.edu

package galileo.util;

import java.awt.BasicStroke;
import math.geom2d.Point2D;
import math.geom2d.polygon.Polygon2D;
import math.geom2d.polygon.SimplePolygon2D;
import java.awt.EventQueue;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Polygon;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.awt.geom.Line2D;
import java.util.Collection;

import javax.swing.JFrame;
import javax.swing.JPanel;

import math.geom2d.polygon.SimplePolygon2D;

class Surface extends JPanel {

    private void doDrawing(Graphics g) {
    	Graphics2D g2d = (Graphics2D) g;
    	int x = 150;
        int y = 25;
    	
    	g2d.drawLine(x, y, x, y);
    	//g2d.drawLine(x-10, y, x-10, y+30);
    	
    	int xPoints[] = {25, 145, 145, 25 , 95};
        int yPoints[] = {25, 25, 145, 145, 65};
        //lineDraw(xPoints, yPoints, g2d);
        double d = getDistance(xPoints[1], yPoints[1], x, y)*2;
        
        centroidCalc(xPoints, yPoints, g2d);
        
        Polygon p = new Polygon(xPoints, yPoints, 5);
        
        //Point pt = new Point(x, y);
        Line2D.Double double1 = new Line2D.Double(x,y,x,y);
       
        //Stroke stroke = new BasicStroke((int)(d+0.5));
        //g2d.setStroke(stroke);
        
        
        g2d.drawPolygon(p);
        
        generateOuterPolygon(xPoints, yPoints, g2d);
        
        
        
        /*AffineTransform scaleMatrix = new AffineTransform();
        scaleMatrix.translate(10, 10);
        
        g2d.setTransform(scaleMatrix);
        g2d.draw(p);
        
        scaleMatrix = new AffineTransform();
        scaleMatrix.translate(10, 0);
        g2d.setTransform(scaleMatrix);
        g2d.draw(p);
        
        
        Area a  = new Area(p);
        Area b = new Area(double1);
        
        a.intersect(b);
        
        System.out.println(a.isEmpty());*/
        
        
        
        
        
        System.out.println(d);
    }
    
    
    public void generateOuterPolygon(int[] xPoints, int[] yPoints, Graphics2D g2d) {
    	
	    SimplePolygon2D polygon = createPolygon(xPoints, yPoints);
	    
	    Collection<Point2D> bufferPoints = polygon.buffer(2).boundary().vertices();
	    
	    
	    
	    int len = bufferPoints.size();
	    int xPoints1[] = new int[len];
        int yPoints1[] = new int[len];
	    
        int i=0;
        
	    for (Point2D point2D : bufferPoints) {
	    	
	    	xPoints1[i] = (int)(point2D.x());
	    	yPoints1[i] = (int)(point2D.y());
	        System.out.println(point2D.x()+" "+ point2D.y());
	        i++;
	    }
	    
	    Polygon p = new Polygon(xPoints1, yPoints1, len);
	    g2d.drawPolygon(p);

	}
    
    private SimplePolygon2D createPolygon(int[] xPoints, int[] yPoints)
	{
		
	    SimplePolygon2D polygon = new SimplePolygon2D();
	    for (int i=0; i< xPoints.length ; i++)
	    {
	        polygon.addVertex(new Point2D(xPoints[i], yPoints[i]));
	    }
	    return polygon;
	}
	
    
    
    public void lineDraw(int[] xPoints, int[] yPoints, Graphics2D g2d) {
    	AffineTransform saveTransform = g2d.getTransform();
    	Line2D.Double l1 = new Line2D.Double(xPoints[0], yPoints[0], xPoints[1], yPoints[1]);
    	Line2D.Double l2 = new Line2D.Double(xPoints[1], yPoints[1], xPoints[2], yPoints[2]);
    	
    	Line2D.Double l3 = new Line2D.Double(100, 140, 75, 75);
    	g2d.draw(l3);
    	
    	AffineTransform scaleMatrix = new AffineTransform();
        scaleMatrix.translate(10, 10);

        g2d.setTransform(scaleMatrix);
        g2d.draw(l3);
    	
    	
    	
    }
    
    
    public void centroidCalc(int[] x, int[] y, Graphics2D g2d) {
    	
    	double cx = 0;
    	double cy = 0;
    	double a = 0;
    	
    	for(int i = 0; i< x.length-1; i++) {
    		
    		cx +=(x[i]+x[i+1])*((x[i]*y[i+1]) - (x[i+1]*y[i]));
    		cy +=(y[i]+y[i+1])*((x[i]*y[i+1]) - (x[i+1]*y[i]));
    		a += (x[i]*y[i+1]) - (x[i+1]*y[i]);
    		
    	}
    	
    	a = a/2;
    	
    	cx = cx/(6*a);
    	cy = cy/(6*a);
    	
    	Point pt = new Point((int)cx, (int)cy);
    	g2d.drawLine((int)cx, (int)cy, (int)cx, (int)cy);
    	
    }
    
    public void doDrawing1(Graphics g) {

        Graphics2D g2d = (Graphics2D) g;
        AffineTransform saveTransform = g2d.getTransform();
        int x = 150;
        int y = 25;
    	
    	g2d.drawLine(x, y, x, y);
    	g2d.drawLine(x-10, y, x-10, y+30);

        try {
        	
            int xPoints[] = {25, 145, 145, 25 , 35};
            int yPoints[] = {25, 25, 145, 145, 35};
            double d = getDistance(xPoints[1], yPoints[1], x, y)*2;
            
            
            
            //double d = getDistance(xPoints[1], yPoints[1], x, y)*2;
            
            Polygon p = new Polygon(xPoints, yPoints, 5);
            g2d.drawPolygon(p);
            
            AffineTransform scaleMatrix = new AffineTransform();
            scaleMatrix.scale(1.1, 1.1);

            g2d.setTransform(scaleMatrix);
            g2d.drawPolygon(p);
            
            
        } finally {
            g2d.setTransform(saveTransform);
        }
   }
    
    

    @Override
    public void paintComponent(Graphics g) {

        super.paintComponent(g);
        doDrawing(g);
    }
    
    public static double getDistance(float x1, float y1, float x2, float y2)
    {
      // using long to avoid possible overflows when multiplying
      double dx = x2 - x1;
      double dy = y2 - y1;
      
      double x = java.lang.Math.sqrt(dx * dx + dy * dy);

      // return Math.hypot(x2 - x1, y2 - y1); // Extremely slow
      // return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2)); // 20 times faster than hypot
      return x; // 10 times faster then previous line
    }
}

public class BasicEx extends JFrame {

    public BasicEx() {

        initUI();
    }

    private void initUI() {

        add(new Surface());

        setTitle("Simple Java 2D example");
        setSize(1000, 1000);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    public static void main(String[] args) {

        /*EventQueue.invokeLater(new Runnable() {

            @Override
            public void run() {
                BasicEx ex = new BasicEx();
                ex.setVisible(true);
            }
        });*/
    	BasicEx ex = new BasicEx();
        ex.setVisible(true);
    	
    }
}