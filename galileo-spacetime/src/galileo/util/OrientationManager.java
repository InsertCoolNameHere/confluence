package galileo.util;

import java.util.ArrayList;
import java.util.List;

public class OrientationManager {
	
	// nw,n,ne,e,c,w,sw,s,se
	// up, c, down
	private boolean ignore = false;
	private List<Integer> chunks = new ArrayList<Integer>();
	
	public OrientationManager() {
		for(int i=0; i < 27; i++) {
			chunks.set(i, 0);
		}
	}
	
	/**
	 * This returns the smaller chunks needed to build up this particular block part
	 * 
	 * @author sapmitra
	 * @param orientation
	 * @return
	 */
	public static List<Integer> getRequiredChunks(String orientation) {
		
		String[] tokens = orientation.split("-");
		String spatial = tokens[0];
		String temporal = tokens[1];
		
		List<Integer> s = new ArrayList<Integer>();
		List<Integer> t= new ArrayList<Integer>();
		
		/*if(spatial.contains("full") && temporal.contains("full")) {
			// returning null means whole block is required, not chunks
			
			return null;
		}*/
		
		if("nw".equals(spatial)) {
			s.add(1) ;
			
		} else if("ne".equals(spatial)) {
			s.add(3) ;
			
		} else if("sw".equals(spatial)) {
			s.add(7) ;
			
		} else if("se".equals(spatial)) {
			s.add(9);
			
		} else if("n".equals(spatial)) {
			s.add(1);s.add(2);s.add(3);
			
		} else if("s".equals(spatial)) {
			s.add(7);s.add(8);s.add(9);
			
		} else if("e".equals(spatial)) {
			s.add(3);s.add(6);s.add(9);
			
		} else if("w".equals(spatial)) {
			s.add(1);s.add(4);s.add(7);
			
		} else if("full".equals(spatial)) {
			s.add(1);s.add(2);s.add(3);s.add(4);s.add(5);s.add(6);s.add(7);s.add(8);s.add(9);
			
		}
		
		
		if("start".equals(temporal)) {
			t.add(1);
		} else if("full".equals(temporal)) {
			t.add(1);t.add(2);t.add(3);
		} else if("end".equals(temporal)) {
			t.add(3);
		} 
		
		List<Integer> finalChunks = new ArrayList<Integer> ();
		
		for(int j: t){
			for(int i: s) {
				int jump = 9*(j-1);
				finalChunks.add((i+jump) - 1);
			}
		}
		
		return finalChunks;
		
	}
	 
	/**
	 * This will get called only in cases where not the full block only is required
	 * The purpose of this function is to return the part of a block this chunk number represents
	 * 
	 * @author sapmitra
	 * @param chunks
	 * @param bp
	 * @return
	 */
	public static List<Integer> getRecordsFromBlock(List<Integer> chunks, BorderingProperties bp) {
		
		String[] spatials = {"nw","n","ne","e","c","w","sw","s","se"};
		String[] temporals = {"start","mid","end"};
		
		for(int i : chunks) {
			
			int spatialNumber = i % 9 ;
			System.out.println(spatials[spatialNumber]);
			int temporalNumber = i / 9 ;
			System.out.println(temporals[temporalNumber]);
			
		}
		return null;
		
	}
	
	public static void main(String arg[]) {
		
		List<Integer> chunks = new ArrayList<>();
		chunks.add(24);
		
		System.out.println(OrientationManager.getRequiredChunks("sw-end"));
		System.out.println(OrientationManager.getRecordsFromBlock(chunks,null));
		
	}

}
