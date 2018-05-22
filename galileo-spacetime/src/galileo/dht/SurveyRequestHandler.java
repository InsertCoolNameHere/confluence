package galileo.dht;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.comm.DataIntegrationFinalResponse;
import galileo.comm.DataIntegrationResponse;
import galileo.comm.GalileoEventMap;
import galileo.comm.SurveyEventResponse;
import galileo.comm.SurveyResponse;
import galileo.comm.TrainingDataEvent;
import galileo.comm.TrainingDataResponse;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventContext;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.net.RequestListener;
import galileo.serialization.SerializationException;
import galileo.util.MyPorter;

/**
 * This class will collect the responses from all the nodes of galileo and then
 * transfers the result to the listener. Used by the {@link StorageNode} class.
 * 
 * @author sapmitra
 */
public class SurveyRequestHandler implements MessageListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private GalileoEventMap eventMap;
	private BasicEventWrapper eventWrapper;
	private ClientMessageRouter router;
	private AtomicInteger expectedResponses;
	private Collection<NetworkDestination> nodes;
	private EventContext clientContext;
	private List<GalileoMessage> responses;
	private RequestListener requestListener;
	private Event response;
	private long elapsedTime;
	private int numTrainingPoints;
	private AtomicInteger expectedTrainingResponses;
	private String allTrainingData;
	private String fsName;
	private String featureName;
	private double latEps,lonEps,timeEps;
	private String storagePath;
	private String currentNode;
	private boolean hasModel;
	private String model;
	
	

	public SurveyRequestHandler(Collection<NetworkDestination> nodes, EventContext clientContext,
			int numTrainingPoints, String fsName, String featureName, double d, double e, double f, 
			String trainingResultsDir, String nodeString, RequestListener listener, boolean hasModel, String model) throws IOException {
		this.nodes = nodes;
		this.clientContext = clientContext;
		this.requestListener = listener;

		this.router = new ClientMessageRouter(true);
		this.router.addListener(this);
		this.responses = new ArrayList<GalileoMessage>();
		this.eventMap = new GalileoEventMap();
		this.eventWrapper = new BasicEventWrapper(this.eventMap);
		this.expectedResponses = new AtomicInteger(this.nodes.size());
		this.numTrainingPoints = numTrainingPoints;
		this.expectedTrainingResponses = new AtomicInteger(0);
		this.fsName = fsName;
		this.featureName = featureName;
		this.latEps = d;
		this.lonEps = e;
		this.timeEps = f;
		this.allTrainingData = "";
		String eventId = String.valueOf(System.currentTimeMillis());
		this.storagePath = trainingResultsDir + "/trainingData" + eventId;
		this.currentNode = nodeString;
		
		this.hasModel = hasModel;
		
		this.model = model;
		
	}

	public void closeRequest() {
		silentClose(); // closing the router to make sure that no new responses
						// are added.
		
		try {
			if(this.response instanceof SurveyResponse) {
				
				SurveyResponse actualResponse = (SurveyResponse) this.response;
				
				actualResponse.setOutputPath(this.storagePath+".blk");
				actualResponse.setNodeString(currentNode);
			}
				
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"An unknown exception occurred while processing the response message. Details follow:"
							+ e.getMessage(), e);
		}
		
		this.requestListener.onRequestCompleted(this.response, clientContext, this);
	}

	@Override
	public void onMessage(GalileoMessage message) {
		try {
			
			Event event = this.eventWrapper.unwrap(message);
			
			// SURVEY RESPONSE CAME IN
			if(event instanceof SurveyEventResponse) {
				
				logger.log(Level.INFO, "RIKI: SURVEY LEVEL 1 RESPONSE RECEIVED");
				if (null != message)
					this.responses.add(message);
				int awaitedResponses = this.expectedResponses.decrementAndGet();
				logger.log(Level.INFO, "Awaiting " + awaitedResponses + " more message(s)");
				
				if (awaitedResponses <= 0) {
					this.elapsedTime = System.currentTimeMillis() - this.elapsedTime;
					
					List<String> pathInfos = new ArrayList<String>();
					List<String> blocks = new ArrayList<String>();
					List<Long> recordCounts = new ArrayList<Long>();
					
					performStartifiedSamplingAndRequest(pathInfos, blocks, recordCounts);
				}
				
			} else if (event instanceof TrainingDataResponse) {
				
				int awaitedResponses = this.expectedTrainingResponses.decrementAndGet();
				TrainingDataResponse rsp = (TrainingDataResponse)event;
				
				logger.log(Level.INFO, "RIKI: TRAINING DATA RESPONSE RECEIVED");
				if(rsp.getDataPoints() != null && rsp.getDataPoints().length() > 0)
					allTrainingData+= rsp.getDataPoints();
				
				/* The close will happen when the second set of requests have been replied to */
				
				if (awaitedResponses <= 0) {
					
					FileOutputStream fos = null;
					try {
						fos = new FileOutputStream(this.storagePath+".blk");
						fos.write("lat,long,epoch,beta,err,actval\n".getBytes());
						fos.write(allTrainingData.getBytes("UTF-8"));
						fos.close();
						
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Something went wrong while storing training data to the filesystem.", e);
						this.storagePath = null;
					}
					
					this.elapsedTime = System.currentTimeMillis() - this.elapsedTime;
					logger.log(Level.INFO, "Closing the survey request and sending back the response.");
					new Thread() {
						public void run() {
							SurveyRequestHandler.this.closeRequest();
						}
					}.start();
				}
			}
		} catch (IOException | SerializationException e) {
			logger.log(Level.SEVERE, "An exception occurred while processing the response message. Details follow:"
					+ e.getMessage(), e);
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"An unknown exception occurred while processing the response message. Details follow:"
							+ e.getMessage(), e);
		}
	}

	/**
	 * Perform stratified sampling based on number of records and send request for training data
	 * @author sapmitra
	 * @param pathInfos
	 * @param blocks
	 * @param recordCounts
	 */
	private void performStartifiedSamplingAndRequest(List<String> pathInfos, List<String> blocks, List<Long> recordCounts) {
		
		long totalRecordCount = 0;
		int expectedTrainingMsgs = 0;
		
		// PathInfos and blocks has same number of entries
		for (GalileoMessage gresponse : this.responses) {
			Event event;
			try {
				event = this.eventWrapper.unwrap(gresponse);
				if(event instanceof SurveyEventResponse) {
					
					SurveyEventResponse eventResponse = (SurveyEventResponse) event;
					
					if(eventResponse.getPathInfos() != null && eventResponse.getPathInfos().size() > 0) {
						pathInfos.addAll(eventResponse.getPathInfos());
						blocks.addAll(eventResponse.getBlocks());
						recordCounts.addAll(eventResponse.getRecordCounts());
						
						for(long recC : eventResponse.getRecordCounts())
							totalRecordCount+= recC;
					}
					
				}
			} catch (IOException | SerializationException e) {
				logger.log(Level.SEVERE, "An exception occurred while processing the response message. Details follow:"
						+ e.getMessage(), e);
			} catch (Exception e) {
				logger.log(Level.SEVERE,
						"An unknown exception occurred while processing the response message. Details follow:"
								+ e.getMessage(), e);
			}
		}
		
		// BUILDING THE NODEWISE REQUEST
		Map<NetworkDestination, TrainingRequirements> nodeWiseRequest = new HashMap<NetworkDestination, TrainingRequirements>();
		
		// STRATIFIED SAMPLING AND REQUEST CREATED IN ONE GO
		// for each block in each path in each node
		for(int i=0; i < pathInfos.size(); i++) {
			
			String pathInfo = pathInfos.get(i);
			System.out.println("RIKI:PATH RECV "+pathInfo);
			NetworkDestination nd = extractNodeInfo(pathInfo);
			
			// The number of training points to be extracted from this block
			double fraction = (double)recordCounts.get(i)/(double)totalRecordCount;
			
			int numPoints = (int)(Math.floor(fraction*numTrainingPoints));
			logger.info("FRACTION: "+ fraction+" "+numPoints);
			
			String block = blocks.get(i);
			
			if(numPoints > 1) {
				logger.info("NUMPOINTS "+ block+" "+numPoints);
				TrainingRequirements tr;
				if(nodeWiseRequest.get(nd) == null) {
					expectedTrainingMsgs++;
					tr = new TrainingRequirements(new ArrayList<String>(), new ArrayList<Integer>(), new ArrayList<String>());
					nodeWiseRequest.put(nd, tr);
				} else {
					tr = nodeWiseRequest.get(nd);
				}
				tr.addPathInfo(pathInfo);
				tr.addBlockPath(block);
				tr.addNumPoints(numPoints);
				
			}
			
		}
		this.expectedTrainingResponses = new AtomicInteger(expectedTrainingMsgs);
		
		// SEND OUT TRAINING DATA REQUEST
		for(NetworkDestination nd : nodeWiseRequest.keySet()) {
			
			TrainingRequirements tr = nodeWiseRequest.get(nd);
			TrainingDataEvent tde = new TrainingDataEvent(tr, fsName, featureName, latEps, lonEps, timeEps, hasModel, model);
			GalileoMessage mrequest;
			
			try {
				
				mrequest = this.eventWrapper.wrap(tde);
				this.router.sendMessage(nd, mrequest);
				logger.info("RIKI: TRAINING DATA REQUEST SENT TO " + nd.toString());
				
			} catch (IOException e) {
				logger.severe("ERROR OCCURED WHILE REQUESTING FOR TRAINING DATA: "+e.toString());
			}
			
		}
		
	}
	
	private NetworkDestination extractNodeInfo(String pathInfo) {
		String nodePort = pathInfo.split("\\$\\$")[0];
		String[] tokens = nodePort.split(":");
		String nodeName = tokens[0];
		int port = Integer.valueOf(tokens[1]);
		
		for(NetworkDestination n : nodes) {
			
			if(n.getHostname().equals(nodeName) && n.getPort() == port) {
				return n;
			}
		}
		return null;
		
	}

	public static void main(String arg[]) {
		String s = "hello:world".split(":")[0];
		System.out.println(s);
		List<String> sa = new ArrayList<String>();
		sa.addAll(null);
		
	}
	/**
	 * Handles the client request on behalf of the node that received the
	 * request
	 * 
	 * @param request
	 *            - This must be a server side event: Generic Event or
	 *            QueryEvent
	 * @param response
	 */
	public void handleRequest(Event request, Event response) {
		try {
			this.response = response;
			GalileoMessage mrequest = this.eventWrapper.wrap(request);
			for (NetworkDestination node : nodes) {
				this.router.sendMessage(node, mrequest);
				logger.info("Request sent to " + node.toString());
			}
			this.elapsedTime = System.currentTimeMillis();
		} catch (IOException e) {
			logger.log(Level.INFO,
					"Failed to send request to other nodes in the network. Details follow: " + e.getMessage());
		}
	}

	public void silentClose() {
		try {
			this.router.forceShutdown();
		} catch (Exception e) {
			logger.log(Level.INFO, "Failed to shutdown the completed client request handler: ", e);
		}
	}

	@Override
	public void onConnect(NetworkDestination endpoint) {

	}

	@Override
	public void onDisconnect(NetworkDestination endpoint) {

	}
}
