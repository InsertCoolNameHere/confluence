package galileo.util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class MDC {
	
	List<Integer> aValidEntries = new ArrayList<Integer>();
	List<Integer> bValidEntries = new ArrayList<Integer>();
	
	public static void main(String arg[]) {
		List<String[]> aRecords = new ArrayList<>();
		String[] a1 = {"1486354807", "39.473305", "-94.807891", "1"}; 
		aRecords.add(a1);
		
		String[] a2 = {"1486354867","39.483905","-94.902649","2"}; 
		aRecords.add(a2);
		
		String[] a3 = {"1486362067","39.377833","-94.916381","3"}; 
		aRecords.add(a3);
		
		String[] a4 = {"1486362071","39.540058","-94.904022","4"}; 
		aRecords.add(a4);
		
		String[] a5 = {"1486354817","39.461643","-94.641723","5"}; 
		aRecords.add(a5);
		
		String[] a6 = {"1486354801","39.442556","-94.598165","6"}; 
		aRecords.add(a6);
		
		String[] a7 = {"1486354667","39.431950","-94.757080","7"}; 
		aRecords.add(a7);
		
		String[] a8 = {"1486354217","39.428768","-94.858703","8"}; 
		aRecords.add(a8);
		
		
		//,"1,5,7","4,3,2","16,1,12","7,7,7"};
		String bRecords = "1486362069,39.372535,-94.923934,3\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"1486354871,39.44520783,-94.932174,5\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"\n" + 
				"";
		int[] aPosns = {0,1,2};
		int[] bPosns = {0,1,2};
		double[] epsilons = {100,0.05,0.05};
		
		MDC m = new MDC();
		System.out.println(System.currentTimeMillis());
		System.out.println(m.iterativeMultiDimJoin(aRecords, bRecords, aPosns, bPosns, epsilons));
		System.out.println(System.currentTimeMillis());
	}
	
	/*public static void main(String arg[]) {
		String[] ss = new String[3];
		ss[0] = "hi";
		ss[1] = "hello";
		ss[2] = "lo";
		
		System.out.println(java.util.Arrays.toString(ss));
	}*/
	
	
	public List<String> iterativeMultiDimJoin(List<String[]> indvARecords,/*String aRecords,*/ String bRecords, int[] aPosns, int[] bPosns, double[] epsilons) {
		System.out.println("BRECORDS: "+bRecords);
		System.out.println("ARECORDS: ");
		for(String[] sa: indvARecords) {
			System.out.println(Arrays.asList(sa));
		}
		
		System.out.println("APOSNS:");
		for(int i: aPosns) {
			System.out.print(i+" ");
		}
		
		System.out.println();
		
		System.out.println("BPOSNS:");
		for(int i: bPosns) {
			System.out.print(i+" ");
		}
		
		System.out.println();
		System.out.println("EPSILONS:");
		for(double i: epsilons) {
			System.out.print(i+" ");
		}
		
		System.out.println();
		/* Do not modify these 2 data */
		String doublePattern = "-?([0-9]*)\\.?([0-9]*)";
		String intPattern = "-?([0-9])([0-9]*)";
		
		//String[] indvARecords = aRecords.split("\\$\\$");
		String[] indvBRecords = bRecords.split("\\n");
		

		List<double[]> splitARecords = new ArrayList<double[]>();
		List<double[]> splitBRecords = new ArrayList<double[]>();
		
		/* splitARecords and validAEntries must always correspond */
		
		/* Loading & filtering */
		
		int ind = 0;
		
		for(String[] frs: indvARecords) {
			
			if(Pattern.matches(doublePattern, frs[aPosns[1]]) && Pattern.matches(doublePattern, frs[aPosns[2]])
					&& Pattern.matches(intPattern, frs[aPosns[0]])){
				double[] tempArr = new double[3];
				tempArr[0] = Double.valueOf(frs[aPosns[0]]);
				tempArr[1] = Double.valueOf(frs[aPosns[1]]);
				tempArr[2] = Double.valueOf(frs[aPosns[2]]);
				splitARecords.add(tempArr);
				aValidEntries.add(ind);
			}
			
			ind++;
		}
		
		ind = 0;
		for(String line: indvBRecords) {
			
			if(line.trim().isEmpty()) {
				ind++;
				continue;
			}
			line = line.replace(", ", ",");
			String[] frs = line.split(",");
			if(Pattern.matches(doublePattern, frs[bPosns[1]]) && Pattern.matches(doublePattern, frs[bPosns[2]])
					&& Pattern.matches(intPattern, frs[bPosns[0]])){
				
				double[] tempArr = new double[3];
				tempArr[0] = Double.valueOf(frs[bPosns[0]]);
				tempArr[1] = Double.valueOf(frs[bPosns[1]]);
				tempArr[2] = Double.valueOf(frs[bPosns[2]]);
				splitBRecords.add(tempArr);
				bValidEntries.add(ind);
			}
			
			ind++;
		}
		
		
		/* Iterative 1D join */
		List<String> pairs = new ArrayList<String>();
		
		for(int i=0 ; i < 3 ; i++) {
			
			//System.out.println("DIMENSION: "+(i+1)+"\n==============\n");
			/* At all times splitEntries and valid entries should correspond */
			
			List<Integer> validAs = new ArrayList<Integer>(aValidEntries);
			List<Integer> validBs = new ArrayList<Integer>(bValidEntries);
			
			int aPosn = aPosns[i];
			int bPosn = bPosns[i];
			
			
			List<Double> setA = new ArrayList<Double>();
			List<Double> setB = new ArrayList<Double>();
			
			for(double[] frs: splitARecords) {
				
				setA.add(frs[aPosn]);
				
			}
			for(double[] frs: splitBRecords) {
				
				setB.add(frs[bPosn]);
				
			}
			
			ListIndexComparator comparator = new ListIndexComparator(setA, validAs);
			Collections.sort(validAs, comparator);
			Collections.sort(setA);
			
			ListIndexComparator comparator1 = new ListIndexComparator(setB, validBs);
			Collections.sort(validBs, comparator1);
			Collections.sort(setB);
			
			
			List<String> tmpPairs = oneDJoin(setA, validAs, setB, validBs, epsilons[i], splitARecords, splitBRecords);
			
			if(i == 0)
				pairs = tmpPairs;
			else
				pairs.retainAll(tmpPairs);
			
			/* At this point validAs and validBs actually contain invalid entries */
			List<double[]> removalsA = new ArrayList<double[]>();
			for(int in : validAs) {
				int indx = aValidEntries.indexOf(in);
				removalsA.add(splitARecords.get(indx));
			}
			splitARecords.removeAll(removalsA);
			
			List<double[]> removalsB = new ArrayList<double[]>();
			for(int in : validBs) {
				int indx = bValidEntries.indexOf(in);
				//splitBRecords.remove(indx);
				removalsB.add(splitBRecords.get(indx));
			}
			splitBRecords.removeAll(removalsB);
			
			aValidEntries.removeAll(validAs);
			bValidEntries.removeAll(validBs);
			
		}
		
		List<String> retJoinRecords = new ArrayList<String> ();
		for(String line : pairs) {
			
			String[] pr = line.split(",");
			int i1 = Integer.valueOf(pr[0]);
			int i2 = Integer.valueOf(pr[1]);
			
			String ret1 = java.util.Arrays.toString(indvARecords.get(i1));
			String ret2 = indvBRecords[i2];
			
			retJoinRecords.add(ret1+"$$"+ret2);
			
		}
		
		//System.out.println("FINAL ANSWER: "+ pairs);
		return retJoinRecords;
	}
	
	
	
	/* setA and setB are ordered according to the dimension in question */
	public List<String> oneDJoin(List<Double> setA, List<Integer> aInd, List<Double> setB, List<Integer> bInd, 
			double epsilon, List<double[]> splitAs, List<double[]> splitBs) {
		//System.out.println("HERE");
		List<String> pairs = new ArrayList<String>();
		
		List<Integer> aValids = new ArrayList<Integer>();
		List<Integer> bValids = new ArrayList<Integer>();
		
		int aLen = setA.size();
		int bLen = setB.size();
		
		int aCurrIndex = 0;
		int bCurrIndex = 0;
		
		double aStart = setA.get(0);
		double aEnd = setA.get(aLen-1);
		double bStart = setB.get(0);
		double bEnd = setB.get(bLen-1);
		
		double start = aStart;
		
		double end = aEnd;
		
		if(aStart > bStart)
			start = bStart;
		
		if(aEnd < bEnd)
			end = bEnd;
		
		double current = start;
		
		List<Double> setATemp;
		List<Integer> setATempInd;
		List<Double> setBTemp1 = new ArrayList<Double>();
		List<Integer> setBTempInd1 = new ArrayList<Integer>();
		List<Double> setBTemp2 = new ArrayList<Double>();
		List<Integer> setBTempInd2 = new ArrayList<Integer>();
		List<Double> setBTemp3 = new ArrayList<Double>();
		List<Integer> setBTempInd3 = new ArrayList<Integer>();
		
		boolean firstTime = true;
		
		while(current < end) {
			
			//System.out.println("CURRENT:"+current);
			setATemp = new ArrayList<Double>();
			setATempInd = new ArrayList<Integer>();
			
			int acurrIndexBefore = aCurrIndex;
			
			/* load A data - 0 to e */
			while(aCurrIndex < aLen && setA.get(aCurrIndex) <= current+epsilon) {
				setATemp.add(setA.get(aCurrIndex));
				setATempInd.add(aInd.get(aCurrIndex));
				aCurrIndex++;
			}
			
			/* load B data - 0 to e */
			if(firstTime) {
				
				while(bCurrIndex < bLen && setB.get(bCurrIndex) <= current+epsilon) {
					setBTemp2.add(setB.get(bCurrIndex));
					setBTempInd2.add(bInd.get(bCurrIndex));
					bCurrIndex++;
				}
			}
			
			/* load B data - e to 2e */
			while(bCurrIndex < bLen && setB.get(bCurrIndex) <= current+epsilon+epsilon && setB.get(bCurrIndex) > current) {
				
				setBTemp3.add(setB.get(bCurrIndex));
				setBTempInd3.add(bInd.get(bCurrIndex));
				bCurrIndex++;
				
				if(firstTime)
					firstTime = false;
			}
			
			
			current = current+epsilon;
			
			int indxA = 0;
			
			/* Actual Join*/
			/* Only if both A and B entries are non empty */
			if(acurrIndexBefore != aCurrIndex && (!setBTemp1.isEmpty() || !setBTemp2.isEmpty())) {
				for(double d: setATemp) {
					boolean found = false;
					
					int indxB = 0;
					
					for(double dd: setBTemp1) {
						if(java.lang.Math.abs(d-dd)<=epsilon) {
							found = true;
							//System.out.println(setATempInd.get(indxA)+","+setBTempInd1.get(indxB));
							pairs.add(setATempInd.get(indxA)+","+setBTempInd1.get(indxB));
							bValids.add(setBTempInd1.get(indxB));
						}
						indxB++;
					}
					
					if(setBTempInd2.size() > 0) 
						found = true;
					
					for(int i: setBTempInd2) {
						//System.out.println(setATempInd.get(indxA)+","+i);
						pairs.add(setATempInd.get(indxA)+","+i);
						bValids.add(i);
					}
					
					indxB = 0;
					if(!firstTime) {
						for(double dd: setBTemp3) {
							if(java.lang.Math.abs(d-dd)<=epsilon) {
								found = true;
								//System.out.println(setATempInd.get(indxA)+","+setBTempInd3.get(indxB));
								pairs.add(setATempInd.get(indxA)+","+setBTempInd3.get(indxB));
								bValids.add(setBTempInd3.get(indxB));
							}
							indxB++;
						}
					}
					
					if(found)
						aValids.add(setATempInd.get(indxA));
					indxA++;
				}
			}
			
			/* Swap references between b1 & b2 & b3 */
			setBTemp1 = new ArrayList<Double>();
			setBTemp1.addAll(setBTemp2);
			
			setBTempInd1 = new ArrayList<>();
			setBTempInd1.addAll(setBTempInd2);
			
			setBTemp2 = new ArrayList<Double>();
			setBTemp2.addAll(setBTemp3);
			
			setBTempInd2 = new ArrayList<>();
			setBTempInd2.addAll(setBTempInd3);
			
			setBTemp3 = new ArrayList<Double>();
			setBTempInd3 = new ArrayList<Integer>();
			
			
		}
		
		//System.out.println(aValids);
		//System.out.println(bValids);
		
		aInd.removeAll(aValids);
		
		bInd.removeAll(bValids);
		return pairs;
	}


}
