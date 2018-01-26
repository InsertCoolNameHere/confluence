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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.bmp.Bitmap;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityQuery;
import galileo.bmp.QueryTransform;
import galileo.comm.GalileoEventMap;
import galileo.comm.MetadataResponse;
import galileo.comm.NeighborDataEvent;
import galileo.comm.NeighborDataResponse;
import galileo.comm.QueryResponse;
import galileo.event.BasicEventWrapper;
import galileo.event.Event;
import galileo.event.EventContext;
import galileo.fs.GeospatialFileSystem;
import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.net.RequestListener;
import galileo.serialization.SerializationException;
import galileo.util.GeoHash;
import galileo.util.Requirements;
import galileo.util.SuperCube;

/**
 * This class will collect the responses from all the nodes of galileo and then
 * transfers the result to the listener. Used by the {@link StorageNode} class.
 * 
 * @author sapmitra
 */
public class NeighborRequestHandler implements MessageListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private GalileoEventMap eventMap;
	private BasicEventWrapper eventWrapper;
	private ClientMessageRouter router;
	private AtomicInteger expectedControlMessages;
	private Collection<NetworkDestination> nodes;
	private List<NeighborDataEvent> individualRequests;
	private List<NeighborDataEvent> internalEvents;
	private EventContext clientContext;
	private List<GalileoMessage> responses;
	private RequestListener requestListener;
	private Event response;
	private long elapsedTime;
	private List<SuperCube> allCubes;
	private Map<Integer, List<String[]>> fs1SuperCubeDataMap;
	private GeoavailabilityQuery geoQuery;
	private GeospatialFileSystem fs1;
	
	/* This helps track the nuner of control messages coming in */
	private Map<Integer, Integer> superCubeNumNodesMap;
	
	/* Keeping track of how many data messages are coming from a neighbor node */
	private Map<String, Integer> nodeToNumberOfDataMessagesMap;
	/* The values are strings like node+pathid */
	/* Keeping track of which paths are needed by a particular supercube */
	private Map<Integer, List<String>> supercubeToExpectedPathsMap;
	
	private Map<Integer, List<LocalRequirements>> supercubeToRequirementsMap;
	private Map<Integer, List<String>> pathIdToFragmentDataMap;
	private int numCores;
	private Map<Integer, Boolean> superCubeLocalFetchCheck;
	

	public NeighborRequestHandler(List<NeighborDataEvent> internalEvents, List<NeighborDataEvent> individualRequests, Collection<NetworkDestination> destinations, EventContext clientContext,
			RequestListener listener, List<SuperCube> allCubes, Map<Integer, Integer> superCubeNumNodesMap, 
			int numCores, GeoavailabilityQuery geoQuery, GeospatialFileSystem fs1) throws IOException {
		
		this.nodes = destinations;
		this.clientContext = clientContext;
		this.requestListener = listener;
		this.allCubes = allCubes;

		this.internalEvents = internalEvents;
		this.individualRequests = individualRequests;
		this.router = new ClientMessageRouter(true);
		this.router.addListener(this);
		this.responses = new ArrayList<GalileoMessage>();
		this.eventMap = new GalileoEventMap();
		this.eventWrapper = new BasicEventWrapper(this.eventMap);
		this.expectedControlMessages = new AtomicInteger(this.nodes.size());
		
		this.supercubeToExpectedPathsMap = new HashMap<Integer, List<String>>();
		this.nodeToNumberOfDataMessagesMap = new HashMap<String, Integer>();
		this.supercubeToRequirementsMap = new HashMap<Integer, List<LocalRequirements>>();
		this.numCores = numCores;
		this.geoQuery = geoQuery;
		this.fs1 = fs1;
		
		fs1SuperCubeDataMap = new HashMap<Integer, List<String[]>>();
		superCubeLocalFetchCheck = new HashMap<Integer, Boolean>();
		
	}

	public void closeRequest() {
		silentClose(); // closing the router to make sure that no new responses
						// are added.
		class LocalFeature implements Comparable<LocalFeature> {
			String name;
			String type;
			int order;

			LocalFeature(String name, String type, int order) {
				this.name = name;
				this.type = type;
				this.order = order;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null || !(obj instanceof LocalFeature))
					return false;
				LocalFeature other = (LocalFeature) obj;
				if (this.name.equalsIgnoreCase(other.name) && this.type.equalsIgnoreCase(other.type)
						&& this.order == other.order)
					return true;
				return false;
			}

			@Override
			public int hashCode() {
				return name.hashCode() + type.hashCode() + String.valueOf(this.order).hashCode();
			}

			@Override
			public int compareTo(LocalFeature o) {
				return this.order - o.order;
			}
		}
		Map<String, Set<LocalFeature>> resultMap = new HashMap<String, Set<LocalFeature>>();
		int responseCount = 0;

		for (GalileoMessage gresponse : this.responses) {
			responseCount++;
			Event event;
			try {
				event = this.eventWrapper.unwrap(gresponse);
				if (event instanceof QueryResponse && this.response instanceof QueryResponse) {
					QueryResponse actualResponse = (QueryResponse) this.response;
					actualResponse.setElapsedTime(elapsedTime);
					QueryResponse eventResponse = (QueryResponse) event;
					JSONObject responseJSON = actualResponse.getJSONResults();
					JSONObject eventJSON = eventResponse.getJSONResults();
					if (responseJSON.length() == 0) {
						for (String name : JSONObject.getNames(eventJSON))
							responseJSON.put(name, eventJSON.get(name));
					} else {
						if (responseJSON.has("queryId") && eventJSON.has("queryId")
								&& responseJSON.getString("queryId").equalsIgnoreCase(eventJSON.getString("queryId"))) {
							if (actualResponse.isDryRun()) {
								JSONObject actualResults = responseJSON.getJSONObject("result");
								JSONObject eventResults = eventJSON.getJSONObject("result");
								if (null != JSONObject.getNames(eventResults)) {
									for (String name : JSONObject.getNames(eventResults)) {
										if (actualResults.has(name)) {
											JSONArray ar = actualResults.getJSONArray(name);
											JSONArray er = eventResults.getJSONArray(name);
											for (int i = 0; i < er.length(); i++) {
												ar.put(er.get(i));
											}
										} else {
											actualResults.put(name, eventResults.getJSONArray(name));
										}
									}
								}
							} else {
								JSONArray actualResults = responseJSON.getJSONArray("result");
								JSONArray eventResults = eventJSON.getJSONArray("result");
								for (int i = 0; i < eventResults.length(); i++)
									actualResults.put(eventResults.getJSONObject(i));
							}
							if (responseJSON.has("hostProcessingTime")) {
								JSONObject aHostProcessingTime = responseJSON.getJSONObject("hostProcessingTime");
								JSONObject eHostProcessingTime = eventJSON.getJSONObject("hostProcessingTime");

								JSONObject aHostFileSize = responseJSON.getJSONObject("hostFileSize");
								JSONObject eHostFileSize = eventJSON.getJSONObject("hostFileSize");

								for (String key : eHostProcessingTime.keySet())
									aHostProcessingTime.put(key, eHostProcessingTime.getLong(key));
								for (String key : eHostFileSize.keySet())
									aHostFileSize.put(key, eHostFileSize.getLong(key));

								responseJSON.put("totalFileSize",
										responseJSON.getLong("totalFileSize") + eventJSON.getLong("totalFileSize"));
								responseJSON.put("totalNumPaths",
										responseJSON.getLong("totalNumPaths") + eventJSON.getLong("totalNumPaths"));
								responseJSON.put("totalProcessingTime",
										java.lang.Math.max(responseJSON.getLong("totalProcessingTime"),
												eventJSON.getLong("totalProcessingTime")));
								responseJSON.put("totalBlocksProcessed", responseJSON.getLong("totalBlocksProcessed")
										+ eventJSON.getLong("totalBlocksProcessed"));
							}
						}
					}
				} else if (event instanceof MetadataResponse && this.response instanceof MetadataResponse) {
					MetadataResponse emr = (MetadataResponse) event;
					JSONArray emrResults = emr.getResponse().getJSONArray("result");
					JSONObject emrJSON = emr.getResponse();
					if ("galileo#features".equalsIgnoreCase(emrJSON.getString("kind"))) {
						for (int i = 0; i < emrResults.length(); i++) {
							JSONObject fsJSON = emrResults.getJSONObject(i);
							for (String fsName : fsJSON.keySet()) {
								Set<LocalFeature> featureSet = resultMap.get(fsName);
								if (featureSet == null) {
									featureSet = new HashSet<LocalFeature>();
									resultMap.put(fsName, featureSet);
								}
								JSONArray features = fsJSON.getJSONArray(fsName);
								for (int j = 0; j < features.length(); j++) {
									JSONObject jsonFeature = features.getJSONObject(j);
									featureSet.add(new LocalFeature(jsonFeature.getString("name"),
											jsonFeature.getString("type"), jsonFeature.getInt("order")));
								}
							}
						}
						if (this.responses.size() == responseCount) {
							JSONObject jsonResponse = new JSONObject();
							jsonResponse.put("kind", "galileo#features");
							JSONArray fsArray = new JSONArray();
							for (String fsName : resultMap.keySet()) {
								JSONObject fsJSON = new JSONObject();
								JSONArray features = new JSONArray();
								for (LocalFeature feature : new TreeSet<>(resultMap.get(fsName)))
									features.put(new JSONObject().put("name", feature.name).put("type", feature.type)
											.put("order", feature.order));
								fsJSON.put(fsName, features);
								fsArray.put(fsJSON);
							}
							jsonResponse.put("result", fsArray);
							this.response = new MetadataResponse(jsonResponse);
						}
					} else if ("galileo#filesystem".equalsIgnoreCase(emrJSON.getString("kind"))) {
						MetadataResponse amr = (MetadataResponse) this.response;
						JSONObject amrJSON = amr.getResponse();
						if (amrJSON.getJSONArray("result").length() == 0)
							amrJSON.put("result", emrResults);
						else {
							JSONArray amrResults = amrJSON.getJSONArray("result");
							for (int i = 0; i < emrResults.length(); i++) {
								JSONObject emrFilesystem = emrResults.getJSONObject(i);
								for (int j = 0; j < amrResults.length(); j++) {
									JSONObject amrFilesystem = amrResults.getJSONObject(j);
									if (amrFilesystem.getString("name")
											.equalsIgnoreCase(emrFilesystem.getString("name"))) {
										long latestTime = amrFilesystem.getLong("latestTime");
										long earliestTime = amrFilesystem.getLong("earliestTime");
										if (latestTime == 0 || latestTime < emrFilesystem.getLong("latestTime")) {
											amrFilesystem.put("latestTime", emrFilesystem.getLong("latestTime"));
											amrFilesystem.put("latestSpace", emrFilesystem.getString("latestSpace"));
										}
										if (earliestTime == 0 || (earliestTime > emrFilesystem.getLong("earliestTime")
												&& emrFilesystem.getLong("earliestTime") != 0)) {
											amrFilesystem.put("earliestTime", emrFilesystem.getLong("earliestTime"));
											amrFilesystem.put("earliestSpace",
													emrFilesystem.getString("earliestSpace"));
										}
										break;
									}
								}
							}
						}
					} else if ("galileo#overview".equalsIgnoreCase(emrJSON.getString("kind"))) {
						logger.info(emrJSON.getString("kind") + ": emr results length = " + emrResults.length());
						MetadataResponse amr = (MetadataResponse) this.response;
						JSONObject amrJSON = amr.getResponse();
						if (amrJSON.getJSONArray("result").length() == 0)
							amrJSON.put("result", emrResults);
						else {
							JSONArray amrResults = amrJSON.getJSONArray("result");
							for (int i = 0; i < emrResults.length(); i++) {
								JSONObject efsJSON = emrResults.getJSONObject(i);
								String efsName = efsJSON.keys().next();
								JSONObject afsJSON = null;
								for (int j = 0; j < amrResults.length(); j++) {
									if (amrResults.getJSONObject(j).has(efsName)) {
										afsJSON = amrResults.getJSONObject(j);
										break;
									}
								}
								if (afsJSON == null)
									amrResults.put(efsJSON);
								else {
									JSONArray eGeohashes = efsJSON.getJSONArray(efsName);
									JSONArray aGeohashes = afsJSON.getJSONArray(efsName);
									for (int j = 0; j < eGeohashes.length(); j++) {
										JSONObject eGeohash = eGeohashes.getJSONObject(j);
										JSONObject aGeohash = null;
										for (int k = 0; k < aGeohashes.length(); k++) {
											if (aGeohashes.getJSONObject(k).getString("region")
													.equalsIgnoreCase(eGeohash.getString("region"))) {
												aGeohash = aGeohashes.getJSONObject(k);
												break;
											}
										}
										if (aGeohash == null)
											aGeohashes.put(eGeohash);
										else {
											long eTimestamp = eGeohash.getLong("latestTimestamp");
											int blockCount = aGeohash.getInt("blockCount")
													+ eGeohash.getInt("blockCount");
											long fileSize = aGeohash.getInt("fileSize") + eGeohash.getInt("fileSize");
											aGeohash.put("blockCount", blockCount);
											aGeohash.put("fileSize", fileSize);
											if (eTimestamp > aGeohash.getLong("latestTimestamp"))
												aGeohash.put("latestTimestamp", eTimestamp);
										}
									}
								}
							}
						}
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
	
	/* CONTROL MESSAGE */
	private List<Requirements> handleRequirementsOnControlMessage (String requirementsString, int superCubeId, String nodeName) {
		
		String lines[] = requirementsString.trim().split("\\r?\\n");
		
		/* The requirements String of a particular supercube*/
		for(String line: lines) {
			String[] tokens = line.split("-"); 
			if(tokens.length == 2) {
				
				List<String> pathsForThisCube = supercubeToExpectedPathsMap.get(superCubeId);
				if(pathsForThisCube == null) {
					pathsForThisCube = new ArrayList<String>();
				}
				
				int pathIndex = Integer.valueOf(tokens[0]);
				
				String[] fragments = tokens[1].split(",");
				
				pathsForThisCube.add(nodeName+"$"+pathIndex);
				supercubeToExpectedPathsMap.put(superCubeId, pathsForThisCube);
				
				
				
				LocalRequirements lr = createRequirement(pathIndex, fragments);
				
				
				List<LocalRequirements> lrs = supercubeToRequirementsMap.get(superCubeId);
				if(lrs == null) {
					lrs = new ArrayList<LocalRequirements>();
				}
				lrs.add(lr);
				supercubeToRequirementsMap.put(superCubeId, lrs);
				
				
			} else {
				logger.log(Level.SEVERE, "AN INVALID FORMAT FOUND IN CONTROL MESSAGE");
				return null;
			}
		}
		return null;
		
	}
	
	
	private LocalRequirements createRequirement(int pathIndex, String[] fragments) {
		
		LocalRequirements lr = new LocalRequirements(pathIndex,fragments, false);
		return lr;
	}
	
	public static void main(String arg[]) {
		String s = "abc-";
		String[] tokens = s.split("-"); 
		System.out.println(tokens.length);
		
		List<String> ll = new ArrayList<String> ();
		
		ll.add("1");
		ll.add("2");
		ll.remove("3");
		System.out.println(ll.indexOf("1"));
	}

	@Override
	public void onMessage(GalileoMessage message) {
		
		try{
			Event event = this.eventWrapper.unwrap(message);
			
			if(event instanceof NeighborDataResponse) {
				
				NeighborDataResponse rsp = (NeighborDataResponse)event;
				boolean isControlMessage = checkMessageType(rsp);
				
				/* This is a node telling beforehand what is coming */
				if(isControlMessage) {
					/* The machine this response came from */
					String nodeName =rsp.getNodeString();
					
					/* This node is about to send back this many data messages */
					if(rsp.getTotalPaths() > 0)
						nodeToNumberOfDataMessagesMap.put(nodeName, rsp.getTotalPaths());
					
					/* The # of supercubes returns =  number of requirements string */
					for(int superCubeId: rsp.getSupercubeIDList()) {
						int index = rsp.getSupercubeIDList().indexOf(superCubeId);
						
						/* This supercube is expecting response from one less nodes */
						int expectedNodes = superCubeNumNodesMap.get(superCubeId) - 1;
						
						/* One less node to expect control message from */
						superCubeNumNodesMap.put(superCubeId, expectedNodes);
						
						/*Requirements for a single cube came back as 
						 * pathIndex-0,1,2...\n */
						String requirementsString = rsp.getRequirementsList().get(index);
						
						handleRequirementsOnControlMessage(requirementsString, superCubeId, nodeName);
					}
					
					
					/* Update supercube requirements */
				} else {
					/* Update path fragments available */
					/* DATA MESSAGE INCOMING */
					
					List<String> fragmentedRecords = rsp.getResultRecordLists();
					String nodeName = rsp.getNodeString();
					int pathIndex = rsp.getPathIndex();
					
					pathIdToFragmentDataMap.put(pathIndex, fragmentedRecords);
					
					String pathString = nodeName+"$"+pathIndex;
					
					for(int i : supercubeToExpectedPathsMap.keySet()) {
						List<String> expectedPaths = supercubeToExpectedPathsMap.get(i);
						
						expectedPaths.remove(pathString);
						if(expectedPaths.size() <= 0) {
							/* LAUNCH THIS SUPERCUBE INTO A NEW THREAD*/
							logger.log(Level.INFO, "READY TO LAUNCH " + i);
						} 
					}
					
					
					
					
				}
			} else {
				logger.log(Level.SEVERE, "RECEIVED WEIRD NEIGHBOR RESPONSE "+event.getClass());
			}
		}  catch (IOException | SerializationException e) {
			logger.log(Level.SEVERE, "An exception occurred while processing the response message. Details follow:"
					+ e.getMessage(), e);
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"An unknown exception occurred while processing the response message. Details follow:"
							+ e.getMessage(), e);
		}
		
		
		
		/*if (null != message)
			this.responses.add(message);
		int awaitedResponses = this.expectedControlMessages.decrementAndGet();
		logger.log(Level.INFO, "Awaiting " + awaitedResponses + " more message(s)");
		if (awaitedResponses <= 0) {
			this.elapsedTime = System.currentTimeMillis() - this.elapsedTime;
			logger.log(Level.INFO, "Closing the request and sending back the response.");
			new Thread() {
				public void run() {
					NeighborRequestHandler.this.closeRequest();
				}
			}.start();
		}*/
	}
	
	/**
	 * returns true for control message false for data message
	 * @author sapmitra
	 * @param message
	 * @return
	 */
	public boolean checkMessageType(NeighborDataResponse rsp) {
		
		if(!rsp.isActualData()) {
			this.expectedControlMessages.decrementAndGet();
			return true;
		}
		return false;
		
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
	public void handleRequest(Event response) {
		try {
			this.response = response;
			
			/* size of individualRequests must be the same as nodes */
			int count = 0;
			
			// Reading in all fs1 blocks first
			
			LocalReader lr = new LocalReader();
			lr.run();
			//readFS1Blocks(allCubes);
			
			/* TODO: HANDLE INTERNAL EVENTS BEFORE SENDING OUT REQUESTS */
			
			for (NetworkDestination node : nodes) {
				Event request = individualRequests.get(count);
				GalileoMessage mrequest = this.eventWrapper.wrap(request);
				this.router.sendMessage(node, mrequest);
				logger.info("Request sent to " + node.toString());
				count++;
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
	
	/* Reading all blocks supercube by supercube */
	/*public void readFS1Blocks(List<SuperCube> scs) {
		
		try {
			ExecutorService executor = Executors.newFixedThreadPool(Math.min(scs.size(), 2 * numCores));
			List<LocalQueryProcessor> queryProcessors = new ArrayList<LocalQueryProcessor>();
			
			for(SuperCube sc: scs) {
				
				GeoavailabilityGrid blockGrid = new GeoavailabilityGrid(sc.getCentralGeohash(), GeoHash.MAX_PRECISION * 2 / 3);
				
				Bitmap queryBitmap = null;
				
				if (geoQuery.getPolygon() != null)
					queryBitmap = QueryTransform.queryToGridBitmap(geoQuery, blockGrid);
				
				// One of these per SuperCube 
				LocalQueryProcessor qp = new LocalQueryProcessor(fs1, sc.getFs1BlockPath(), geoQuery, blockGrid, queryBitmap, (int)sc.getId());
				queryProcessors.add(qp);
				executor.execute(qp);
					
			}
			executor.shutdown();
			boolean status = executor.awaitTermination(10, TimeUnit.MINUTES);
			if (!status)
				logger.log(Level.WARNING, "Executor terminated because of the specified timeout=10minutes");
			
			for (LocalQueryProcessor qp : queryProcessors) {
				// INDICATING THIS SUPERCUBE IS READY FOR JOIN
				superCubeLocalFetchCheck.put(qp.getSuperCubeId(), true);
				if (qp.getResultRecordLists() != null && qp.getResultRecordLists().size() > 0) {
					fs1SuperCubeDataMap.put(qp.getSuperCubeId(), qp.getResultRecordLists());
				} else {
					fs1SuperCubeDataMap.put(qp.getSuperCubeId(), null);
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Something went wrong while querying FS1 at NeighborRequestHandler", e);
		}
	}*/
	
	class LocalReader implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			readFS1Blocks(allCubes);
		}
		public void readFS1Blocks(List<SuperCube> scs) {
			
			try {
				ExecutorService executor = Executors.newFixedThreadPool(Math.min(scs.size(), 2 * numCores));
				List<LocalQueryProcessor> queryProcessors = new ArrayList<LocalQueryProcessor>();
				
				for(SuperCube sc: scs) {
					
					GeoavailabilityGrid blockGrid = new GeoavailabilityGrid(sc.getCentralGeohash(), GeoHash.MAX_PRECISION * 2 / 3);
					
					Bitmap queryBitmap = null;
					
					if (geoQuery.getPolygon() != null)
						queryBitmap = QueryTransform.queryToGridBitmap(geoQuery, blockGrid);
					
					// One of these per SuperCube 
					LocalQueryProcessor qp = new LocalQueryProcessor(fs1, sc.getFs1BlockPath(), geoQuery, blockGrid, queryBitmap, (int)sc.getId());
					queryProcessors.add(qp);
					executor.execute(qp);
						
				}
				executor.shutdown();
				boolean status = executor.awaitTermination(10, TimeUnit.MINUTES);
				if (!status)
					logger.log(Level.WARNING, "Executor terminated because of the specified timeout=10minutes");
				
				for (LocalQueryProcessor qp : queryProcessors) {
					// INDICATING THIS SUPERCUBE IS READY FOR JOIN
					
					synchronized(superCubeLocalFetchCheck) {
						synchronized(fs1SuperCubeDataMap) {
							superCubeLocalFetchCheck.put(qp.getSuperCubeId(), true);
						
						
							if (qp.getResultRecordLists() != null && qp.getResultRecordLists().size() > 0) {
								fs1SuperCubeDataMap.put(qp.getSuperCubeId(), qp.getResultRecordLists());
							} else {
								fs1SuperCubeDataMap.put(qp.getSuperCubeId(), null);
							}
						}
					}
				}
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Something went wrong while querying FS1 at NeighborRequestHandler", e);
			}
		}
	}
	
	
	
	
	
}
