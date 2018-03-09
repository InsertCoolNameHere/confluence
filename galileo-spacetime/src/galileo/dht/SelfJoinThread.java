package galileo.dht;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import galileo.util.MDC;

public class SelfJoinThread implements Runnable {
	private List<String[]> indvARecords;
	private List<String[]> indvBRecords;
	private double[] epsilons;
	private String trainingPoints;
	private List<Integer> betas;

	
	public SelfJoinThread(List<String[]> indvARecords, List<String[]> indvBRecords, double latEps, double lonEps, double timeEps) {
		this.indvARecords = indvARecords;
		this.indvBRecords = indvBRecords;
		this.epsilons = new double[]{latEps, lonEps, timeEps};
		this.betas = new ArrayList<Integer>(Arrays.asList(2,3,4));
	}

	@Override
	public void run() {
		
		MDC m = new MDC();
		List<String> joinRes = m.iterativeMultiDimSelfJoin(indvARecords, indvBRecords, epsilons, betas);
		// TODO Auto-generated method stub
		
		
	}
		
}