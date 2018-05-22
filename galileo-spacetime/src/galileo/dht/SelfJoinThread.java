package galileo.dht;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import galileo.comm.TemporalType;
import galileo.util.MDC;
import galileo.util.MyPorter;

public class SelfJoinThread implements Runnable {
	// lat,lon,time,feature
	private List<String[]> indvARecords;
	private List<String[]> indvBRecords;
	private double[] epsilons;
	private String trainingPoints;
	private double[] betas;
	private String pathInfo;
	private TemporalType temporalType;
	private static final Logger logger = Logger.getLogger("galileo");
	
	private boolean hasModel = false;
	private MyPorter model;
	
	public SelfJoinThread(List<String[]> indvARecords, List<String[]> indvBRecords,
			double latEps, double lonEps, double timeEps, String pathInfo, double[] dEFAULT_BETAS, TemporalType temporalType) {
		this.indvARecords = indvARecords;
		this.indvBRecords = indvBRecords;
		this.epsilons = new double[]{latEps, lonEps, timeEps};
		this.betas = dEFAULT_BETAS;
		this.pathInfo = pathInfo;
		this.temporalType = temporalType;
		this.trainingPoints = "";
		this.hasModel = false;
		
	}

	public SelfJoinThread(List<String[]> indvARecords, List<String[]> indvBRecords, double latEps, double lonEps,
			double timeEps, String pathInfo, String model, TemporalType temporalType, double[] dEFAULT_BETAS) {
		this.indvARecords = indvARecords;
		this.indvBRecords = indvBRecords;
		this.epsilons = new double[]{latEps, lonEps, timeEps};
		this.hasModel = true;
		this.model = new MyPorter(model);
		this.betas = dEFAULT_BETAS;
		this.pathInfo = pathInfo;
		this.temporalType = temporalType;
		this.trainingPoints = "";
	}

	@Override
	public void run() {
		
		if(indvARecords.size() > 0 && indvBRecords.size() > 0) {
			if(!hasModel) {
			
				MDC m = new MDC();
				List<String> tps = m.iterativeMultiDimSelfJoinML(indvARecords, indvBRecords, epsilons, betas, pathInfo, temporalType, hasModel, null);
				// TODO Auto-generated method stub
				
				StringBuilder sb = new StringBuilder();
				if(tps.size() > 0) {
					for(String tp : tps)
						if(tp.length() > 0)
							sb.append(tp+"\n");
				}
				
				this.trainingPoints = sb.toString();
				
			} else {
				/*int randIndex = ThreadLocalRandom.current().nextInt(0, indvBRecords.size());
				
				String[] rec = indvBRecords.get(randIndex);
				
				// RIKI MARKER
				// THIS BETA NEEDS TO BE CALCULATED AFTER STANDARDIZATION OF INPUT
				double[] ip = {Double.valueOf(rec[0]), Double.valueOf(rec[1]), Double.valueOf(rec[2])};
				double beta = model.predict(ip);
				
				double[] appendedBetas = new double[betas.length+1];
				appendedBetas[0] = beta;
				int c=1;
				for(double b : betas) {
					appendedBetas[c] = b;
					c++;
				}*/
				
				MDC m = new MDC();
				
				List<String> tps = m.iterativeMultiDimSelfJoinML(indvARecords, indvBRecords, epsilons, betas, pathInfo, temporalType, hasModel, model);
				// TODO Auto-generated method stub
				
				StringBuilder sb = new StringBuilder();
				if(tps.size() > 0) {
					for(String tp : tps)
						if(tp.length() > 0)
							sb.append(tp+"\n");
				}
				
				this.trainingPoints = sb.toString();
				
			}
		}
		
	}

	public String getTrainingPoints() {
		return trainingPoints;
	}

	public void setTrainingPoints(String trainingPoints) {
		this.trainingPoints = trainingPoints;
	}
		
}