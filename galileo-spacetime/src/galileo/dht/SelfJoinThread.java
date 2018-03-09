package galileo.dht;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import galileo.util.MDC;

public class SelfJoinThread implements Runnable {
	private List<String[]> indvARecords;
	private List<String[]> indvBRecords;
	private double[] epsilons;
	/*
	 * int[] aPosns; int[] bPosns; double[] epsilons;
	 */
	private String storagePath;
	private List<Integer> allFragsTo26 = new ArrayList<Integer>();

	
	public SelfJoinThread(List<String[]> indvARecords, List<String[]> indvBRecords, double latEps, double lonEps, double timeEps) {
		this.indvARecords = indvARecords;
		this.indvBRecords = indvBRecords;
		this.epsilons = new double[]{latEps, lonEps, timeEps};
	}

	@Override
	public void run() {
		
		MDC m = new MDC();
		List<String> joinRes = m.iterativeMultiDimSelfJoin(indvARecords, indvBRecords, epsilons);
		// TODO Auto-generated method stub
		
		
		if (joinRes.size() > 0) {
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(this.storagePath+".blk");
				for (String res : joinRes) {
					
					fos.write(res.getBytes("UTF-8"));
					
					
					fos.write("\n".getBytes("UTF-8"));
				}
				fos.close();
				
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Something went wrong while storing to the filesystem.", e);
				this.storagePath = null;
			}
		} else {
			this.storagePath = null;
		}
			
		synchronized(resultFiles) {
			if(storagePath != null)
				resultFiles.add(storagePath);
		}
		cubesLeft--;
		
		if(cubesLeft <= 0) {
			// LAUNCH CLOSE REQUEST
			logger.log(Level.INFO, "All Joins finished and saved");
			new Thread() {
				public void run() {
					NeighborRequestHandler.this.closeRequest();
				}
			}.start();
		}
		
	}