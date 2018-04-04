package galileo.util;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import galileo.comm.TemporalType;
import galileo.dataset.SpatialRange;

public class MDCReplacement {
	
	List<Integer> aValidEntries = new ArrayList<Integer>();
	List<Integer> bValidEntries = new ArrayList<Integer>();
	private static final Logger logger = Logger.getLogger("galileo");
	private int mode = 7;
	
	public MDCReplacement(int mode) {
		this.mode = 0;
	}
	
	public MDCReplacement() {this.mode = 7;}
	
	public static void main1(String arg[]) {
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
		
		MDCReplacement m = new MDCReplacement();
		System.out.println(System.currentTimeMillis());
		System.out.println(m.iterativeMultiDimJoin(aRecords, bRecords, aPosns, bPosns, epsilons,3));
		System.out.println(System.currentTimeMillis());
	}
	
	
	public static void main2(String arg[]) {
		List<Integer> aRecordIndices = new ArrayList<Integer>();
		List<List<Integer>> bRecordIndices = new ArrayList<List<Integer>>();
		MDCReplacement m = new MDCReplacement();
		List<String> pairs = new ArrayList<String>();
		pairs.add("1,2");
		pairs.add("1,3");
		pairs.add("1,4");
		pairs.add("2,2");
		pairs.add("2,4");
		pairs.add("3,2");
		pairs.add("3,11");
		
		m.generateNeighborSphere(pairs, aRecordIndices, bRecordIndices);
		System.out.println(aRecordIndices);
		System.out.println(bRecordIndices);
	}
	public static void main(String arg[]) {
		MDCReplacement m = new MDCReplacement();
		
		m.whyIsThisHappening();
		
	}
	
	public void whyIsThisHappening() {
		
		BufferedReader br = null;
		FileReader fr = null;
		List<String[]> indvARecords = new ArrayList<>();
		
		try {

			fr = new FileReader("/s/chopin/b/grad/sapmitra/Documents/Conflux/testJoin1/A11194.txt");
			br = new BufferedReader(fr);

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				if(!sCurrentLine.isEmpty() && sCurrentLine.contains("[") && sCurrentLine.contains("]")) {
					
					sCurrentLine = sCurrentLine.substring(1, sCurrentLine.length() - 1);
					String tokens[] = sCurrentLine.split(",");
					
					int count = 0;
					for(String t : tokens) {
						
						t = t.trim();
						tokens[count] = t;
						count++;
						
					}
					indvARecords.add(tokens);
					
				}
			}
			
			br.close();
			fr.close();

		} catch (IOException e) {

			e.printStackTrace();

		} 
		
		String bRecords="";
		try {

			fr = new FileReader("/s/chopin/b/grad/sapmitra/Documents/Conflux/testJoin1/B11194.txt");
			br = new BufferedReader(fr);

			String sCurrentLine;
			
			
			while ((sCurrentLine = br.readLine()) != null) {
				
				bRecords += sCurrentLine+"\n";
					
				
			}
			
			br.close();
			fr.close();

		} catch (IOException e) {

			e.printStackTrace();

		} 
		
		int[] aPosns = {42,24,25};
		int[] bPosns = {2,0,1};
		double[] epsilons = {1000*60*60, 0.1, 0.1};
		long ll1 = System.currentTimeMillis();
		System.out.println("SIZEEEE"+iterativeMultiDimJoin(indvARecords, bRecords, aPosns, bPosns, epsilons, 3).size());
		System.out.println(System.currentTimeMillis() - ll1);
		
		
	}
	
	public List<String> iterativeMultiDimJoin(List<String[]> indvARecords,/*String aRecords,*/ String bRecords, int[] aPosns, int[] bPosns, double[] epsilons, int interpolatingFeature) {
		
		int aLength = indvARecords.size();
		int bLength = 0;
		
		//int fileNum = indvARecords.size();
		
		//long ll = System.currentTimeMillis()%100;
		/*System.out.println("APOSNS: "+ indvARecords.get(0).length);
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
		}*/
		
		//System.out.println();
		/* Do not modify these 2 data */
		String doublePattern = "-?([0-9]*)\\.?([0-9]*)";
		String intPattern = "-?([0-9])([0-9]*)";
		
		//String[] indvARecords = aRecords.split("\\$\\$");
		String[] indvBRecords = bRecords.split("\\n");
		
		
		// these two records only contain 3 fields, time, lat and long
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
		bLength = ind;
		
		
		/* Iterative 1D join */
		List<String> pairs = new ArrayList<String>();
		
		
		// initializing bitmap
		char[] finalbitmap1 = new char[aLength*bLength];
		char[] finalbitmap2 = new char[aLength*bLength];
		char[] finalbitmap3 = new char[aLength*bLength];
		
		/*for(int i=0; i < aLength*bLength; i++)
			finalbitmap[i] = '0';*/
		
		
		
		for(int i=2 ; i >= 0 ; i--) {
			
			//System.out.println("DIMENSION: "+(i+1)+"\n==============\n");
			/* At all times splitEntries and valid entries should correspond */
			
			List<Integer> validAs = new ArrayList<Integer>(aValidEntries);
			List<Integer> validBs = new ArrayList<Integer>(bValidEntries);
			
			int aPosn = aPosns[i];
			int bPosn = bPosns[i];
			
			/* If no match has been found and setA/ setB has become empty now */
			if(splitARecords.isEmpty() || splitBRecords.isEmpty()) {
				logger.info("GOING OUT FOR NO MATCHES");
				pairs = new ArrayList<String>();
				break;
			}
			
			List<Double> setA = new ArrayList<Double>();
			List<Double> setB = new ArrayList<Double>();
			
			for(double[] frs: splitARecords) {
				//System.out.println("SPLIT A RECORDS: "+Arrays.asList(frs)+" "+frs.length);
				//setA.add(frs[aPosn]);
				setA.add(frs[i]);
				
			}
			for(double[] frs: splitBRecords) {
				
				//setB.add(frs[bPosn]);
				setB.add(frs[i]);
				
			}
			
			
			ListIndexComparator comparator = new ListIndexComparator(setA, validAs);
			Collections.sort(validAs, comparator);
			Collections.sort(setA);
			
			ListIndexComparator comparator1 = new ListIndexComparator(setB, validBs);
			Collections.sort(validBs, comparator1);
			Collections.sort(setB);
			
			// temporary bitmaps
			char[] currentbitmap = new char[aLength*bLength];
			
			for(int j=0; j < aLength*bLength; j++)
				currentbitmap[j] = '0';
			
			long tl = System.currentTimeMillis();
			List<String> tmpPairs = oneDJoin(setA, validAs, setB, validBs, epsilons[i], splitARecords, splitBRecords,
					currentbitmap, bLength);
			//System.out.println("TIME: "+(System.currentTimeMillis() - tl));
			if(i == 2) {
				
				finalbitmap3 = currentbitmap;
			} else if (i == 1) {
				finalbitmap2 = currentbitmap;
				
			} else if (i == 0) {
				
				finalbitmap1 = currentbitmap;
			}
			
			
			//aValidEntries = new ArrayList<Integer>();
			//bValidEntries = new ArrayList<Integer>();
			if(i != 0) {
				
				// THE LATER PART WILL BE REMOVED LATER
				// At this point validAs and validBs actually contain invalid entries 
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
			
		}
	
		
		Collections.sort(aValidEntries);
		Collections.sort(bValidEntries);
		
		// combining bitmaps
		for(int ind1 = 0; ind1 < aLength ; ind1++) {
			if(aValidEntries.contains(ind1)) {
				for(int ind2 = 0; ind2 < bLength; ind2++) {
					if(bValidEntries.contains(ind2)) {
						int bitMapIndex = ind1*bLength + ind2;
						if(finalbitmap1[bitMapIndex] == '1' && finalbitmap2[bitMapIndex] == '1' && finalbitmap3[bitMapIndex] == '1') {
							
							//int x = ind2 / bLength;
							//int y = ind2 % bLength;
							pairs.add(ind1+","+ind2);
							
						} else {
							//finalbitmap[j] = '0';
						}
					}
				}
			}
		}
		
		System.out.println("JOIN HAS FINISHED.... ON TO IDW ");
		
		List<String> retJoinRecords = new ArrayList<String> ();
		
		// all A Points
		List<Integer> aRecordIndices = new ArrayList<Integer>();
		// all B points list for each A point
		List<List<Integer>> bRecordIndices = new ArrayList<List<Integer>>();
		
		// create the one to many mapping between A and B entries
		generateNeighborSphere(pairs, aRecordIndices, bRecordIndices);
		
		// for each entry in aRecords and corresponding neighbors in bRecords, now
		// apply IDW with different betas
		int count = 0;
		for(int aIndex: aRecordIndices) {
			List<Integer> bIndices = bRecordIndices.get(count);
			
			String[] aRec = indvARecords.get(aIndex);
			
			List<String[]> bRecs = new ArrayList<String[]>();
			
			int fullMatch = -1;
			for(int ib : bIndices) {
				String[] entry = indvBRecords[ib].split(",");
				
				// entry with same space and time, 100% accuracy, no IDW needed
				
				if(entry[bPosns[0]] == aRec[aPosns[0]] && entry[bPosns[1]] == aRec[aPosns[1]] && entry[bPosns[2]] == aRec[aPosns[2]]) {
					fullMatch = ib;
					
				}
				
				bRecs.add(entry);
			}
			
			// min and span needs to be passed here
			if(fullMatch >= 0) {
				
				String bString="";
				for(String[] brec: bRecs) {
					bString+=Arrays.asList(brec)+"**";
				}
				String record = Arrays.asList(aRec)+"<SEP>"+bString+"<PRED>"+bRecs.get(fullMatch)[interpolatingFeature];
				retJoinRecords.add(record);
				
			} else {
				double[] betas = {2d};
				if(bRecs.size() > 0) {
					String rec = IDW.calculateIDW(aRec, bRecs, betas, interpolatingFeature, aPosns, bPosns);
					
					retJoinRecords.add(rec);
				} 
			}
			
			//retJoinRecords.add(ret1+"$$"+ret2);
			
			count++;
		}
		
		return retJoinRecords;
	}
	
	
	/**
	 * Specially designed for self join
	 * Used to generate training data
	 * Include functionality for adding range for latitude, longitude and time, for normalization 
	 * 
	 * @author sapmitra
	 * @param indvARecords
	 * @param indvBRecords
	 * @param epsilons
	 * @param betas 
	 * @param pathInfo nodestring$$time$space
	 * @param temporalType 
	 * @return
	 */
	public List<String> iterativeMultiDimSelfJoin(List<String[]> indvARecords, List<String[]> indvBRecords, 
			double[] epsilons, double[] betas, String pathInfo, TemporalType temporalType) {
		
		List<Double> mins = new ArrayList<Double>();
		List<Double> spans = new ArrayList<Double>();
		
		// This deals with values for standardization
		getStandardizationParameters(pathInfo,temporalType, mins, spans);
		
		/* Do not modify these 2 data */
		String doublePattern = "-?([0-9]*)\\.?([0-9]*)";
		String intPattern = "-?([0-9])([0-9]*)";
		
		List<double[]> splitARecords = new ArrayList<double[]>();
		List<double[]> splitBRecords = new ArrayList<double[]>();
		
		int ind = 0;
		
		for(String[] frs: indvARecords) {
			
			if(Pattern.matches(doublePattern, frs[0]) && Pattern.matches(doublePattern, frs[1])
					&& Pattern.matches(intPattern, frs[2])){
				double[] tempArr = new double[3];
				tempArr[0] = Double.valueOf(frs[0]);
				tempArr[1] = Double.valueOf(frs[1]);
				tempArr[2] = Double.valueOf(frs[2]);
				splitARecords.add(tempArr);
				aValidEntries.add(ind);
			}
			
			ind++;
		}
		
		ind = 0;
		for(String[] frs: indvBRecords) {
			
			if(Pattern.matches(doublePattern, frs[0]) && Pattern.matches(doublePattern, frs[1])
					&& Pattern.matches(intPattern, frs[2])){
				double[] tempArr = new double[3];
				tempArr[0] = Double.valueOf(frs[0]);
				tempArr[1] = Double.valueOf(frs[1]);
				tempArr[2] = Double.valueOf(frs[2]);
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
			
			int aPosn = i;
			int bPosn = i;
			
			
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
		
		// all A Points
		List<Integer> aRecordIndices = new ArrayList<Integer>();
		// all B points list for each A point
		List<List<Integer>> bRecordIndices = new ArrayList<List<Integer>>();
		
		// create the one to many mapping between A and B entries
		generateNeighborSphere(pairs, aRecordIndices, bRecordIndices);
		
		// for each entry in aRecords and corresponding neighbors in bRecords, now
		// apply IDW with different betas and generate training points
		int count = 0;
		for(int aIndex: aRecordIndices) {
			List<Integer> bIndices = bRecordIndices.get(count);
			
			String[] aRec = indvARecords.get(aIndex);
			
			List<String[]> bRecs = new ArrayList<String[]>();
			for(int ib : bIndices) {
				String[] entry = indvBRecords.get(ib);
				
				// ignore entry with same space and time, if any
				if(entry[0] == aRec[0] && entry[1] == aRec[1] && entry[2] == aRec[2])
					continue;
				
				bRecs.add(entry);
			}
			
			// min and span needs to be passed here
			
			if(bRecs.size() > 0) {
				String tp = IDW.getOneTrainingPoint(aRec, bRecs, betas, mins.get(0), spans.get(0),
						mins.get(1), spans.get(1),mins.get(2), spans.get(2));
				
				retJoinRecords.add(tp);
			} 
			
			//retJoinRecords.add(ret1+"$$"+ret2);
			
			count++;
		}
		
		return retJoinRecords;
	}

	/**
	 * @param pathInfo
	 * @param temporalType
	 * @param mins
	 * @param spans
	 */
	private void getStandardizationParameters(String pathInfo, TemporalType temporalType, List<Double> mins,
			List<Double> spans) {
		String realPathInfo = pathInfo.split("\\$\\$")[1];
		String[] tokens = realPathInfo.split("\\$");
		String space = tokens[1];
		String time = tokens[0];
		
		String[] timeTokens = time.split("-");
		
		SpatialRange decodeHash = GeoHash.decodeHash(space);
		
		mins.add((double)decodeHash.getLowerBoundForLatitude());
		spans.add((double)decodeHash.getUpperBoundForLatitude() - (double)decodeHash.getLowerBoundForLatitude());
		
		mins.add((double)decodeHash.getLowerBoundForLongitude());
		spans.add((double)decodeHash.getUpperBoundForLongitude() - (double)decodeHash.getLowerBoundForLongitude());
		
		try {
			double startTimeStamp = (double)GeoHash.getStartTimeStamp(timeTokens[0], timeTokens[1], timeTokens[2], timeTokens[3], temporalType);
			double span = 3600*1000;
			switch (temporalType) {
				case HOUR_OF_DAY:
					span = 3600*1000;
					break;
				case DAY_OF_MONTH:
					span = 24*3600*1000;
					break;
				case MONTH:
					span = 30*24*3600*1000;
					break;
				case YEAR:
					span = 365*30*24*3600*1000;
					break;
			}
			mins.add(startTimeStamp);
			spans.add(span);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param pairs
	 * @param aRecordIndices
	 * @param bRecordIndices
	 */
	private void generateNeighborSphere(List<String> pairs, List<Integer> aRecordIndices,
			List<List<Integer>> bRecordIndices) {
		for(String line : pairs) {
			
			String[] pr = line.split(",");
			int i1 = Integer.valueOf(pr[0]);
			int i2 = Integer.valueOf(pr[1]);
			
			//String ret1 = java.util.Arrays.toString(indvARecords.get(i1));
			//String ret2 = java.util.Arrays.toString(indvBRecords.get(i2));
			
			//retJoinRecords.add(ret1+"$$"+ret2);
			
			int index = -1;
			List<Integer> neighborIndices;
			
			if(aRecordIndices.contains(i1)) {
				index = aRecordIndices.indexOf(i1);
				neighborIndices = bRecordIndices.get(index);
			} else {
				index = aRecordIndices.size();
				aRecordIndices.add(i1);
				neighborIndices = new ArrayList<Integer>();
				bRecordIndices.add(neighborIndices);
			}
			neighborIndices.add(i2);
			
		}
	}
	
	
	
	/* setA and setB are ordered according to the dimension in question */
	public List<String> oneDJoin(List<Double> setA, List<Integer> aInd, List<Double> setB, List<Integer> bInd, 
			double epsilon, List<double[]> splitAs, List<double[]> splitBs) {
		//System.out.println("RIKI HERE");
		List<String> pairs = new ArrayList<String>();
		
		List<Integer> aValids = new ArrayList<Integer>();
		List<Integer> bValids = new ArrayList<Integer>();
		
		int aLen = setA.size();
		int bLen = setB.size();
		
		//System.out.println("# SET A ENTRIES "+aLen);
		//System.out.println("# SET B ENTRIES "+bLen);
		
		int aCurrIndex = 0;
		int bCurrIndex = 0;
		
		double aStart = setA.get(0);
		double aEnd = setA.get(aLen-1);
		double bStart = setB.get(0);
		double bEnd = setB.get(bLen-1);
		
		double start = aStart;
		
		double end = aEnd;
		
		if(aStart > bStart) {
			if(aStart - epsilon > bStart)
				start = aStart - epsilon;
			else
				start = bStart;
		}
		
		if(aEnd < bEnd) {
			if(aEnd + epsilon < bEnd)
				end = aEnd + epsilon;
			else
				end = bEnd;
		}
		
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
	
	// REAL JOIN
	public List<String> oneDJoin(List<Double> setA, List<Integer> aInd, List<Double> setB, List<Integer> bInd, 
			double epsilon, List<double[]> splitAs, List<double[]> splitBs, char[] bitMap, int roundVal) {
		//System.out.println("RIKI HERE");
		List<String> pairs = new ArrayList<String>();
		
		List<Integer> aValids = new ArrayList<Integer>();
		List<Integer> bValids = new ArrayList<Integer>();
		
		int aLen = setA.size();
		int bLen = setB.size();
		
		//System.out.println("# SET A ENTRIES "+aLen);
		//System.out.println("# SET B ENTRIES "+bLen);
		
		int aCurrIndex = 0;
		int bCurrIndex = 0;
		
		double aStart = setA.get(0);
		double aEnd = setA.get(aLen-1);
		double bStart = setB.get(0);
		double bEnd = setB.get(bLen-1);
		
		double start = aStart;
		
		double end = aEnd;
		
		if(aStart > bStart) {
			if(aStart - epsilon > bStart)
				start = aStart - epsilon;
			else
				start = bStart;
		}
		
		if(aEnd < bEnd) {
			if(aEnd + epsilon < bEnd)
				end = aEnd + epsilon;
			else
				end = bEnd;
		}
		
		double current = start;
		
		// The sets and related setInd correspond
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
							
							// PAIRS ADDING
							int x = setATempInd.get(indxA);
							int y = setBTempInd1.get(indxB);
							bitMap[x*roundVal+y] = '1';
							bValids.add(setBTempInd1.get(indxB));
							
						}
						indxB++;
					}
					
					if(setBTempInd2.size() > 0) 
						found = true;
					
					for(int i: setBTempInd2) {
						//System.out.println(setATempInd.get(indxA)+","+i);
						
						// PAIRS ADDING
						int x = setATempInd.get(indxA);
						int y = i;
						bitMap[x*roundVal+y] = '1';
						
						bValids.add(i);	
						
					}
					
					indxB = 0;
					if(!firstTime) {
						for(double dd: setBTemp3) {
							if(java.lang.Math.abs(d-dd)<=epsilon) {
								found = true;
								//System.out.println(setATempInd.get(indxA)+","+setBTempInd3.get(indxB));
								
								int x = setATempInd.get(indxA);
								int y = setBTempInd3.get(indxB);
								bitMap[x*roundVal+y] = '1';
								
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
		
		
		aInd.removeAll(aValids);
		bInd.removeAll(bValids);
		return pairs;
	}
	
	
	public void addToBitMap(int[][] bitmap, int round, int i, int j) {
		
		if(round == 2) {
			bitmap[i][j] = 1;
		} else if (round == 1) {
			if(bitmap[i][j] == 1) {
				
			}
		}
		
	}


}
