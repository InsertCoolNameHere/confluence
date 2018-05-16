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
		this.trainingPoints = "";
		
	}

	@Override
	public void run() {
		
		MDC m = new MDC();
		List<String> tps = m.iterativeMultiDimSelfJoinML(indvARecords, indvBRecords, epsilons, betas, pathInfo, temporalType);
		// TODO Auto-generated method stub
		
		
		StringBuilder sb = new StringBuilder();
		if(tps.size() > 0) {
			for(String tp : tps)
				if(tp.length() > 0)
					sb.append(tp+"\n");
		}
		
		this.trainingPoints = sb.toString();
		
	}

	public String getTrainingPoints() {
		return trainingPoints;
	}

	public void setTrainingPoints(String trainingPoints) {
		this.trainingPoints = trainingPoints;
	}
		
}