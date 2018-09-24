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

public class MDC {
	
	public List<Integer> aValidEntries = new ArrayList<Integer>();
	public List<Integer> bValidEntries = new ArrayList<Integer>();
	
	public static double INVALID_VAL = 9990d;
	
	private static final Logger logger = Logger.getLogger("galileo");
	private int mode = 7;
	
	public MDC(int mode) {
		this.mode = 0;
	}
	
	public MDC() {this.mode = 7;}
	
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
		
		MDC m = new MDC();
		System.out.println(System.currentTimeMillis());
		System.out.println(m.iterativeMultiDimJoin(aRecords, bRecords, aPosns, bPosns, epsilons,3));
		System.out.println(System.currentTimeMillis());
	}
	
	
	public static void main2(String arg[]) {
		List<Integer> aRecordIndices = new ArrayList<Integer>();
		List<List<Integer>> bRecordIndices = new ArrayList<List<Integer>>();
		MDC m = new MDC();
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
		MDC m = new MDC();
		
		m.whyIsThisHappening();
		
	}
	
	public void whyIsThisHappeningSelf() {
		
		BufferedReader br = null;
		FileReader fr = null;
		List<String[]> indvARecords = new ArrayList<>();
		List<String[]> indvBRecords = new ArrayList<>();
		
		try {

			fr = new FileReader("/s/chopin/b/grad/sapmitra/Documents/Conflux/testJoin1/A.txt");
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
		
		
		try {

			fr = new FileReader("/s/chopin/b/grad/sapmitra/Documents/Conflux/testJoin1/B.txt");
			br = new BufferedReader(fr);

			String sCurrentLine;
			
			
			while ((sCurrentLine = br.readLine()) != null) {
				
				
				
				sCurrentLine = sCurrentLine.substring(1, sCurrentLine.length() - 1);
				String tokens[] = sCurrentLine.split(",");
				
				int count = 0;
				for(String t : tokens) {
					
					t = t.trim();
					tokens[count] = t;
					count++;
					
				}
				indvBRecords.add(tokens);
				
			
					
				
			}
			
			br.close();
			fr.close();

		} catch (IOException e) {

			e.printStackTrace();

		} 
		
		double[] betas = {0,1,2};
		double[] epsilons = {1000*60*10, 0.1, 0.1};
		long ll1 = System.currentTimeMillis();
		List<String> recs = iterativeMultiDimSelfJoinML(indvARecords, indvBRecords, epsilons, betas, "abc$$2016-05-12-xx$9v00", TemporalType.DAY_OF_MONTH, false, null);
		System.out.println(recs.size());
		System.out.println(recs);
		
		
	}
	
	public void whyIsThisHappening() {
		
		BufferedReader br = null;
		FileReader fr = null;
		List<String[]> indvARecords = new ArrayList<>();
		
		try {

			fr = new FileReader("/s/chopin/b/grad/sapmitra/Documents/Conflux/testJoin/nam.gblock");
			br = new BufferedReader(fr);

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				if(!sCurrentLine.isEmpty()) {
					
					//sCurrentLine = sCurrentLine.substring(1, sCurrentLine.length() - 1);
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

			fr = new FileReader("/s/chopin/b/grad/sapmitra/Documents/Conflux/testJoin/noaa.gblock");
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
		
		int[] aPosns = {0,1,2};
		int[] bPosns = {0,1,4};
		double[] epsilons = {1000*60*30, 0.01, 0.01};
		long ll1 = System.currentTimeMillis();
		int size = iterativeMultiDimJoin(indvARecords, bRecords, aPosns, bPosns, epsilons, 3).size();
		System.out.println("SIZE "+size);
		long ll2 = System.currentTimeMillis() - ll1;
		System.out.println(ll2);
		
		
	}
	
	/* ACTUAL JOIN */
	public List<String> iterativeMultiDimJoin(List<String[]> indvARecords,/*String aRecords,*/ String bRecords, int[] aPosns, int[] bPosns, double[] epsilons, int interpolatingFeature) {
		int aLength = indvARecords.size();
		int bLength = 0;
	
		long startTime = System.currentTimeMillis();
		String[] indvBRecords = bRecords.split("\\n");
		
		if(indvARecords.isEmpty() || bRecords.isEmpty()) {
			logger.info("GOING OUT FOR NO MATCHES");
			return new ArrayList<String>();
		}
		/* splitARecords and validAEntries must always correspond */
		
		/* Loading & filtering */
		
		int ind = 0;
		int i = 2;
		
		List<Double> setA = new ArrayList<Double>();
		List<Double> setB = new ArrayList<Double>();
		
		for(String[] frs: indvARecords) {
			
			aValidEntries.add(ind);
			setA.add(Double.valueOf(frs[aPosns[i]]));
			ind++;
		}
		
		ind = 0;
		for(String line: indvBRecords) {
			
			if(line.trim().isEmpty() || !line.contains(", ")) {
				ind++;
				continue;
			}
			//line = line.replace(", ", ",");
			String[] frs = line.split(", ");
			bValidEntries.add(ind);
			if(frs.length < i) {
				System.out.println("PROBLEMATIC: "+line);
			} else {
				setB.add(Double.valueOf(frs[bPosns[i]]));
			}
			
			ind++;
		}
		bLength = ind;
		
		//logger.info("RIKI: BRECORDS are:"+bRecords+"ZZZZ" );
		//logger.info("RIKI: ARECORDS are "+Arrays.toString(indvARecords.get(0)));
		//logger.info("=========================");
		/* Iterative 1D join */
		List<String> pairs = new ArrayList<String>();
		
		
		// initializing bitmap
		char[] finalbitmap = new char[aLength*bLength];
		
		
		/* If no match has been found and setA/ setB has become empty now */
		if(bValidEntries.isEmpty() || aValidEntries.isEmpty()) {
			logger.info("GOING OUT FOR NO MATCHES");
			pairs = new ArrayList<String>();
			return pairs;
		}
		
		
		
		/*for(String[] frs: indvARecords) {
			//System.out.println("SPLIT A RECORDS: "+Arrays.asList(frs)+" "+frs.length);
			//setA.add(frs[aPosn]);
			setA.add(Double.valueOf(frs[aPosns[i]]));
			
		}
		for(String ln: indvBRecords) {
			String[] frs = ln.split(",");
			//setB.add(frs[bPosn]);
			setB.add(Double.valueOf(frs[bPosns[i]]));
			
		}*/
		
		
		ListIndexComparator comparator = new ListIndexComparator(setA, aValidEntries);
		Collections.sort(aValidEntries, comparator);
		Collections.sort(setA);
		
		ListIndexComparator comparator1 = new ListIndexComparator(setB, bValidEntries);
		Collections.sort(bValidEntries, comparator1);
		Collections.sort(setB);
		
		
		oneDJoin(i, setA, aValidEntries, setB, bValidEntries, epsilons, indvARecords, indvBRecords, finalbitmap, bLength, aPosns, bPosns);
		
		
		// all A Points
		List<Integer> aRecordIndices = new ArrayList<Integer>();
		// all B points list for each A point
		List<List<Integer>> bRecordIndices = new ArrayList<List<Integer>>();
	
		
		//Collections.sort(aValidEntries);
		//Collections.sort(bValidEntries);
		// combining bitmaps
		for(int ind1 = 0; ind1 < aLength ; ind1++) {
			//if(aValidEntries.contains(ind1)) {
				for(int ind2 = 0; ind2 < bLength; ind2++) {
					//if(bValidEntries.contains(ind2)) {
						int bitMapIndex = ind1*bLength + ind2;
						if(finalbitmap[bitMapIndex] == '1') {
							
							//int x = ind2 / bLength;
							//int y = ind2 % bLength;
							//pairs.add(ind1+","+ind2);
							
							//String ret1 = java.util.Arrays.toString(indvARecords.get(i1));
							//String ret2 = java.util.Arrays.toString(indvBRecords.get(i2));
							
							//retJoinRecords.add(ret1+"$$"+ret2);
							
							int index = -1;
							List<Integer> neighborIndices;
							
							if(aRecordIndices.contains(ind1)) {
								index = aRecordIndices.indexOf(ind1);
								neighborIndices = bRecordIndices.get(index);
							} else {
								index = aRecordIndices.size();
								aRecordIndices.add(ind1);
								neighborIndices = new ArrayList<Integer>();
								bRecordIndices.add(neighborIndices);
							}
							neighborIndices.add(ind2);
						}
					//}
				//}
			}
		}
		
		
		//logger.info("RIKI: A RECORD MATCHES "+aRecordIndices);
		
		List<String> retJoinRecords = new ArrayList<String> ();
		
		long startTimeInterpolation  = System.currentTimeMillis();
		logger.info("RIKI: JOIN FINISHED IN: "+(startTimeInterpolation - startTime));

		
		// create the one to many mapping between A and B entries
		//generateNeighborSphere(pairs, aRecordIndices, bRecordIndices);
		
		if(aRecordIndices.size() > 0) {
			// for each entry in aRecords and corresponding neighbors in bRecords, now
			// apply IDW with different betas
			int count = 0;
			for(int aIndex: aRecordIndices) {
				List<Integer> bIndices = bRecordIndices.get(count);
				
				if(bIndices.size() <= 0) {
					count++;
					continue;
					
				}
				
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
		}
		long endTimeInterpolation  = System.currentTimeMillis();
		logger.info("RIKI: INTERPOLATION FINISHED IN: "+(endTimeInterpolation - startTimeInterpolation));

		aValidEntries = null;
		bValidEntries = null;
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
	 * @param model 
	 * @return
	 */
	public List<String> iterativeMultiDimSelfJoinML(List<String[]> indvARecords, List<String[]> indvBRecords, 
			double[] epsilons, double[] betas, String pathInfo, TemporalType temporalType, boolean hasModel, MyPorter model) {
		
		int aLength = indvARecords.size();
		int bLength = indvBRecords.size();
		
		//System.out.println("TEST A: "+ Arrays.asList(indvARecords.get(0)));
		//System.out.println("TEST B: "+ Arrays.asList(indvBRecords.get(0)));
		
		List<Double> mins = new ArrayList<Double>();
		List<Double> spans = new ArrayList<Double>();
		
		List<Double> setA = new ArrayList<Double>();
		List<Double> setB = new ArrayList<Double>();
		
		// This deals with values for standardization
		getStandardizationParameters(pathInfo,temporalType, mins, spans);
		
		int ind = 0;
		int i=2;
		
		int[] aPosns = {0,1,2};
		int[] bPosns = {0,1,2};
		
		for(String[] frs: indvARecords) {
			aValidEntries.add(ind);
			setA.add(Double.valueOf(frs[aPosns[i]]));
			ind++;
		}
		
		ind = 0;
		
		for(String[] frs: indvBRecords) {
			bValidEntries.add(ind);
			setB.add(Double.valueOf(frs[bPosns[i]]));
			ind++;
		}
		
		
		// initializing bitmap
		char[] finalbitmap = new char[aLength*bLength];
		
		/* Iterative 1D join */
		List<String> pairs = new ArrayList<String>();
		
		if(bValidEntries.isEmpty() || aValidEntries.isEmpty()) {
			logger.info("GOING OUT FOR NO MATCHES");
			//pairs = new ArrayList<String>();
			return pairs;
		}
		
		ListIndexComparator comparator = new ListIndexComparator(setA, aValidEntries);
		Collections.sort(aValidEntries, comparator);
		Collections.sort(setA);
		
		ListIndexComparator comparator1 = new ListIndexComparator(setB, bValidEntries);
		Collections.sort(bValidEntries, comparator1);
		Collections.sort(setB);
		
		
		oneDSelfJoinML(i, setA, aValidEntries, setB, bValidEntries, epsilons, indvARecords, indvBRecords, finalbitmap, bLength, aPosns, bPosns);
			
		// all A Points
		List<Integer> aRecordIndices = new ArrayList<Integer>();
		// all B points list for each A point
		List<List<Integer>> bRecordIndices = new ArrayList<List<Integer>>();
	
		
		for (int ind1 = 0; ind1 < aLength; ind1++) {
			for (int ind2 = 0; ind2 < bLength; ind2++) {
				int bitMapIndex = ind1 * bLength + ind2;
				if (finalbitmap[bitMapIndex] == '1') {
					
					int index = -1;
					List<Integer> neighborIndices;

					if (aRecordIndices.contains(ind1)) {
						index = aRecordIndices.indexOf(ind1);
						neighborIndices = bRecordIndices.get(index);
					} else {
						index = aRecordIndices.size();
						aRecordIndices.add(ind1);
						neighborIndices = new ArrayList<Integer>();
						bRecordIndices.add(neighborIndices);
					}
					if(ind2 != ind1)
						neighborIndices.add(ind2);
				}
			}
		}
		// JOIN FINISHED
		
		List<String> retJoinRecords = new ArrayList<String> ();
		
		// create the one to many mapping between A and B entries
		//generateNeighborSphere(pairs, aRecordIndices, bRecordIndices);
		
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
				if((entry[0] == aRec[0] && entry[1] == aRec[1] && entry[2] == aRec[2]) || Double.valueOf(entry[3]) >= INVALID_VAL 
						|| Double.valueOf(aRec[3]) >= INVALID_VAL)
					continue;
				
				bRecs.add(entry);
			}
			
			// min and span needs to be passed here
			if(!hasModel) {
				// only if multiple neighbors are there
				if(bRecs.size() > 1) {
					String tp = IDW.getOneTrainingPoint(aRec, bRecs, betas, mins.get(0), spans.get(0),
							mins.get(1), spans.get(1),mins.get(2), spans.get(2));
					
					retJoinRecords.add(tp);
				} 
			} else {
				
				double[] ip = {Double.valueOf(aRec[0]), Double.valueOf(aRec[1]), Double.valueOf(aRec[2])};
				//double beta = java.lang.Math.floor(model.predict(ip));
				double beta = model.predict(ip);
				
				
				double[] appendedBetas = new double[betas.length+1];
				appendedBetas[0] = beta;
				int c=1;
				for(double b : betas) {
					appendedBetas[c] = b;
					c++;
				}
				
				// only if multiple neighbors are there
				if(bRecs.size() > 1 && Double.valueOf(aRec[3]) < INVALID_VAL) {
					String tp = IDW.getOneComparison(aRec, bRecs, appendedBetas, mins.get(0), spans.get(0),
							mins.get(1), spans.get(1),mins.get(2), spans.get(2));
					
					retJoinRecords.add(tp);
				} 
				
			}
			
			//retJoinRecords.add(ret1+"$$"+ret2);
			
			count++;
		}
		//logger.info("RIKI:COUNT "+count);
		return retJoinRecords;
	}

	private void oneDSelfJoinML(int currentInd, List<Double> setA, List<Integer> aInd, List<Double> setB,
			List<Integer> bInd, double[] epsilons, List<String[]> indvARecords, List<String[]> indvBRecords,
			char[] bitMap, int roundVal, int[] aPosns, int[] bPosns) {
		
		double epsilon = epsilons[currentInd];
		
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
							
							String[] tokensA = indvARecords.get(x);
							//String line = indvBRecords[y];//
							//line = line.replace(", ", ",");
							String[] tokensB = indvBRecords.get(y);
							
							if(java.lang.Math.abs(Double.valueOf(tokensA[aPosns[0]]) - Double.valueOf(tokensB[bPosns[0]]))<= epsilons[0]
									&& java.lang.Math.abs(Double.valueOf(tokensA[aPosns[1]]) - Double.valueOf(tokensB[bPosns[1]]))<= epsilons[1]) {
								
								bitMap[x*roundVal+y] = '1';
							}
							
							
							//bValids.add(setBTempInd1.get(indxB));
							
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
						
						
						String[] tokensA = indvARecords.get(x);
						//String line = indvBRecords[y];
						//line = line.replace(", ", ",");
						String[] tokensB = indvBRecords.get(y);
						
						if(java.lang.Math.abs(Double.valueOf(tokensA[aPosns[0]]) - Double.valueOf(tokensB[bPosns[0]]))<= epsilons[0]
								&& java.lang.Math.abs(Double.valueOf(tokensA[aPosns[1]]) - Double.valueOf(tokensB[bPosns[1]]))<= epsilons[1]) {
							
							bitMap[x*roundVal+y] = '1';
						}
						//bitMap[x*roundVal+y] = '1';
						
						//bValids.add(i);	
						
					}
					
					indxB = 0;
					if(!firstTime) {
						for(double dd: setBTemp3) {
							if(java.lang.Math.abs(d-dd)<=epsilon) {
								found = true;
								//System.out.println(setATempInd.get(indxA)+","+setBTempInd3.get(indxB));
								
								int x = setATempInd.get(indxA);
								int y = setBTempInd3.get(indxB);
								
								
								String[] tokensA = indvARecords.get(x);
								//String line = indvBRecords[y];
								//line = line.replace(", ", ",");
								String[] tokensB = indvBRecords.get(y);
								
								if(java.lang.Math.abs(Double.valueOf(tokensA[aPosns[0]]) - Double.valueOf(tokensB[bPosns[0]]))<= epsilons[0]
										&& java.lang.Math.abs(Double.valueOf(tokensA[aPosns[1]]) - Double.valueOf(tokensB[bPosns[1]]))<= epsilons[1]) {
									
									bitMap[x*roundVal+y] = '1';
								}
								
								
								
								//bitMap[x*roundVal+y] = '1';
								
								//bValids.add(setBTempInd3.get(indxB));
							}
							indxB++;
						}
					}
					 if(found)	
						 //aValids.add(setATempInd.get(indxA));
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
	public List<String> oneDJoinML(List<Double> setA, List<Integer> aInd, List<Double> setB, List<Integer> bInd, 
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
	public void oneDJoin(int currentInd, List<Double> setA, List<Integer> aInd, List<Double> setB, List<Integer> bInd, 
			double[] epsilons, List<String[]> indvARecords, String[] indvBRecords, char[] bitMap, int roundVal, int[] aPosns, int[] bPosns) {
		//System.out.println("RIKI HERE");
		//List<String> pairs = new ArrayList<String>();
		double epsilon = epsilons[currentInd];
		
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
							
							String[] tokensA = indvARecords.get(x);
							String line = indvBRecords[y];
							//line = line.replace(", ", ",");
							String[] tokensB = line.split(", ");
							
							if(java.lang.Math.abs(Double.valueOf(tokensA[aPosns[0]]) - Double.valueOf(tokensB[bPosns[0]]))<= epsilons[0]
									&& java.lang.Math.abs(Double.valueOf(tokensA[aPosns[1]]) - Double.valueOf(tokensB[bPosns[1]]))<= epsilons[1]) {
								
								bitMap[x*roundVal+y] = '1';
							}
							
							
							//bValids.add(setBTempInd1.get(indxB));
							
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
						
						
						String[] tokensA = indvARecords.get(x);
						String line = indvBRecords[y];
						line = line.replace(", ", ",");
						String[] tokensB = line.split(",");
						
						if(java.lang.Math.abs(Double.valueOf(tokensA[aPosns[0]]) - Double.valueOf(tokensB[bPosns[0]]))<= epsilons[0]
								&& java.lang.Math.abs(Double.valueOf(tokensA[aPosns[1]]) - Double.valueOf(tokensB[bPosns[1]]))<= epsilons[1]) {
							
							bitMap[x*roundVal+y] = '1';
						}
						//bitMap[x*roundVal+y] = '1';
						
						//bValids.add(i);	
						
					}
					
					indxB = 0;
					if(!firstTime) {
						for(double dd: setBTemp3) {
							if(java.lang.Math.abs(d-dd)<=epsilon) {
								found = true;
								//System.out.println(setATempInd.get(indxA)+","+setBTempInd3.get(indxB));
								
								int x = setATempInd.get(indxA);
								int y = setBTempInd3.get(indxB);
								
								
								String[] tokensA = indvARecords.get(x);
								String line = indvBRecords[y];
								line = line.replace(", ", ",");
								String[] tokensB = line.split(",");
								
								if(java.lang.Math.abs(Double.valueOf(tokensA[aPosns[0]]) - Double.valueOf(tokensB[bPosns[0]]))<= epsilons[0]
										&& java.lang.Math.abs(Double.valueOf(tokensA[aPosns[1]]) - Double.valueOf(tokensB[bPosns[1]]))<= epsilons[1]) {
									
									bitMap[x*roundVal+y] = '1';
								}
								
								
								
								//bitMap[x*roundVal+y] = '1';
								
								//bValids.add(setBTempInd3.get(indxB));
							}
							indxB++;
						}
					}
					 if(found)	
						 //aValids.add(setATempInd.get(indxA));
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
		
		
		//aInd.removeAll(aValids);
		//bInd.removeAll(bValids);
		
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
