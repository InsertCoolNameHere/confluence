package galileo.util;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

public class OrientationManager {
	
	// nw,n,ne,e,c,w,sw,s,se
	// down, mid, up
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
		
		
		if("down".equals(temporal)) {
			t.add(1);
		} else if("full".equals(temporal)) {
			t.add(1);t.add(2);t.add(3);
		} else if("up".equals(temporal)) {
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
	public static List<Long> getRecordNumbersFromBlock(int chunk, BorderingProperties bp) {
		
		String[] spatials = {"nw","n","ne","w","c","e","sw","s","se"};
		String[] temporals = {"down","mid","up"};
			
		int spatialNumber = chunk % 9 ;
		//System.out.println("SPATIAL NUM: "+chunk + " "+spatials[spatialNumber]);
		List<Long> spatialRecordNums = getSpecificRecords(spatials[spatialNumber], 1, bp);
		//System.out.println("SPATIAL REC NUM: "+chunk + " "+spatialRecordNums);
		
		int temporalNumber = chunk / 9 ;
		//System.out.println("TEMPORAL NUM: "+chunk + " "+temporals[temporalNumber]);
		//System.out.println(temporals[temporalNumber]);
		List<Long> temporalRecordNums = getSpecificRecords(temporals[temporalNumber], 2, bp);
		/*System.out.println("SPATIAL NUM: "+chunk + " "+spatials[spatialNumber]+
				"\nSPATIAL REC NUM: "+chunk + " "+spatialRecordNums+
				"\nTEMPORAL NUM: "+chunk + " "+temporals[temporalNumber]+
				"\nTEMPORAL REC NUM: "+chunk + " "+temporalRecordNums);*/
			
		if(spatialRecordNums == null || temporalRecordNums == null || spatialRecordNums.size() <= 0 || temporalRecordNums.size() <= 0 )
			return null;
		
		spatialRecordNums.retainAll(temporalRecordNums);
		return spatialRecordNums;
		
	}
	
	public static List<Long> getSpecificRecords(String dir, int type, BorderingProperties bp) {
		/* For spatial */
		if(type == 1) {
			
			if(dir.equals("nw")) {
				return bp.getNwEntries();
			} else if(dir.equals("n")) {
				return bp.getNorthEntries();
			} else if(dir.equals("ne")) {
				return bp.getNeEntries();
			} else if(dir.equals("e")) {
				return bp.getEastEntries();
			} else if(dir.equals("w")) {
				return bp.getWestEntries();
			} else if(dir.equals("sw")) {
				return bp.getSwEntries();
			} else if(dir.equals("s")) {
				return bp.getSouthEntries();
			} else if(dir.equals("se")) {
				return bp.getSeEntries();
			} else if(dir.equals("c")) {
				long totalRecords = bp.getTotalRecords();
				
				List<Long> allEntries = new ArrayList<Long>(ContiguousSet.create(Range.closed(0l, totalRecords - 1), DiscreteDomain.longs()));
				allEntries.removeAll(bp.getNorthEntries());
				allEntries.removeAll(bp.getSouthEntries());
				allEntries.removeAll(bp.getEastEntries());
				allEntries.removeAll(bp.getWestEntries());
				
				return allEntries;
			} 
			
			
		}
		
		/* For Temporal */
		/* Remember the "end" part of a time span is referred to as "up" in Bordering properties
		 * because it is actually the upper timestamp */
		
		if(type == 2) {
			if(dir.equals("down")) {
				return bp.getDownTimeEntries();
			} else if(dir.equals("up")) {
				return bp.getUpTimeEntries();
			} else if(dir.equals("mid")) {
				long totalRecords = bp.getTotalRecords();
				List<Long> allEntries = new ArrayList<Long>(ContiguousSet.create(Range.closed(0l, totalRecords - 1), DiscreteDomain.longs()));
				allEntries.removeAll(bp.getUpTimeEntries());
				allEntries.removeAll(bp.getDownTimeEntries());
				return allEntries;
			}
			
		}
		
		return null;
	}
	
	public static void main(String arg[]) {
		System.out.println(OrientationManager.getRequiredChunks("e-full"));
		List<Integer> chunks = new ArrayList<>();
		chunks.add(24);
		List<Long> allEntries = new ArrayList<Long>(ContiguousSet.create(Range.closed(0l, 2l), DiscreteDomain.longs()));
		System.out.println(allEntries);
		//System.out.println(OrientationManager.getRecordNumbersFromBlock(24,null));
		
		
	}

}
