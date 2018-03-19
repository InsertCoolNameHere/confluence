package galileo.dht;

import java.util.List;

import galileo.comm.TemporalType;
import galileo.util.MDC;

public class SelfJoinThread implements Runnable {
	private List<String[]> indvARecords;
	private List<String[]> indvBRecords;
	private double[] epsilons;
	private String trainingPoints;
	private double[] betas;
	private String pathInfo;
	private TemporalType temporalType;
	
	public SelfJoinThread(List<String[]> indvARecords, List<String[]> indvBRecords,
			double latEps, double lonEps, double timeEps, String pathInfo, double[] dEFAULT_BETAS, TemporalType temporalType) {
		this.indvARecords = indvARecords;
		this.indvBRecords = indvBRecords;
		this.epsilons = new double[]{latEps, lonEps, timeEps};
		this.betas = dEFAULT_BETAS;
		this.pathInfo = pathInfo;
		this.temporalType = temporalType;
		
	}

	@Override
	public void run() {
		
		MDC m = new MDC();
		List<String> tps = m.iterativeMultiDimSelfJoin(indvARecords, indvBRecords, epsilons, betas, pathInfo, temporalType);
		// TODO Auto-generated method stub
		for(String tp : tps)
			if(tp.length() > 0)
				this.trainingPoints += (tp+"\n");
		
	}

	public String getTrainingPoints() {
		return trainingPoints;
	}

	public void setTrainingPoints(String trainingPoints) {
		this.trainingPoints = trainingPoints;
	}
		
}