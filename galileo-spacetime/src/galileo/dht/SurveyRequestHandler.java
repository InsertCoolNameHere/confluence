package galileo.dht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.comm.DataIntegrationFinalResponse;
import galileo.comm.DataIntegrationResponse;
import galileo.comm.GalileoEventMap;
import galileo.comm.MetadataResponse;
import galileo.comm.QueryResponse;
import galileo.comm.SurveyEventResponse;
import galileo.comm.SurveyResponse;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventContext;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.net.RequestListener;
import galileo.serialization.SerializationException;

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
	private AtomicInteger expectedDataTesponses;
	private Collection<NetworkDestination> nodes;
	private EventContext clientContext;
	private List<GalileoMessage> responses;
	private RequestListener requestListener;
	private Event response;
	private long elapsedTime;
	private int numTrainingPoints;

	public SurveyRequestHandler(Collection<NetworkDestination> nodes, EventContext clientContext,
			int numTrainingPoints, RequestListener listener) throws IOException {
		this.nodes = nodes;
		this.clientContext = clientContext;
		this.requestListener = listener;

		this.router = new ClientMessageRouter(true);
		this.router.addListener(this);
		this.responses = new ArrayList<GalileoMessage>();
		this.eventMap = new GalileoEventMap();
		this.eventWrapper = new BasicEventWrapper(this.eventMap);
		this.expectedResponses = new AtomicInteger(this.nodes.size());
		this.expectedDataTesponses = new AtomicInteger(0);
		this.numTrainingPoints = numTrainingPoints;
	}

	public void closeRequest() {
		silentClose(); // closing the router to make sure that no new responses
						// are added.
		
		for (GalileoMessage gresponse : this.responses) {
			Event event;
			try {
				event = this.eventWrapper.unwrap(gresponse);
				if(event instanceof DataIntegrationResponse && this.response instanceof DataIntegrationFinalResponse) {
					
					DataIntegrationFinalResponse actualResponse = (DataIntegrationFinalResponse) this.response;
					
					DataIntegrationResponse eventResponse = (DataIntegrationResponse) event;
					
					for(String path: eventResponse.getResultPaths()) {
						
						String newPath = eventResponse.getNodeName()+":"+eventResponse.getNodePort()+"$$"+path;
						actualResponse.addResultPath(newPath);
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
		this.requestListener.onRequestCompleted(this.response, clientContext, this);
	}

	@Override
	public void onMessage(GalileoMessage message) {
		
		logger.log(Level.INFO, "RIKI: Survey Level 1 RESPONSE RECEIVED");
		if (null != message)
			this.responses.add(message);
		int awaitedResponses = this.expectedResponses.decrementAndGet();
		logger.log(Level.INFO, "Awaiting " + awaitedResponses + " more message(s)");
		
		if (awaitedResponses <= 0) {
			this.elapsedTime = System.currentTimeMillis() - this.elapsedTime;
			
			List<String> pathInfos = new ArrayList<String>();
			List<String> blocks = new ArrayList<String>();
			List<Long> recordCounts = new ArrayList<Long>();
			
			performStartifiedSampling(pathInfos, blocks, recordCounts);
		}
		
		/* The close will happen when the second set of requests have been replied to */
		// TO BE CHANGED ***************************
		if (awaitedResponses <= 0) {
			this.elapsedTime = System.currentTimeMillis() - this.elapsedTime;
			logger.log(Level.INFO, "Closing the request and sending back the response.");
			new Thread() {
				public void run() {
					SurveyRequestHandler.this.closeRequest();
				}
			}.start();
		}
	}

	private void performStartifiedSampling(List<String> pathInfos, List<String> blocks,
			List<Long> recordCounts) {
		
		for (GalileoMessage gresponse : this.responses) {
			Event event;
			try {
				event = this.eventWrapper.unwrap(gresponse);
				if(event instanceof SurveyEventResponse) {
					
					SurveyEventResponse eventResponse = (SurveyEventResponse) event;
					
					pathInfos.addAll(eventResponse.getPathInfos());
					blocks.addAll(eventResponse.getBlocks());
					recordCounts.addAll(eventResponse.getRecordCounts());
					
					
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
