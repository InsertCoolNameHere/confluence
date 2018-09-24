package galileo.dht;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import galileo.bmp.BitmapException;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityQuery;
import galileo.bmp.QueryTransform;
import galileo.comm.DataIntegrationResponse;
import galileo.comm.GalileoEventMap;
import galileo.comm.MetadataResponse;
import galileo.comm.NeighborDataEvent;
import galileo.comm.NeighborDataResponse;
import galileo.comm.QueryResponse;
import galileo.dataset.Coordinates;
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
import galileo.util.MDC;
import galileo.util.MyPorter;
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
	private List<Integer> cleanupCandidateCubes;
	private Map<Integer, List<String[]>> fs1SuperCubeDataMap;
	private GeoavailabilityQuery geoQuery;
	private GeospatialFileSystem fs1;
	private String eventId;
	private String queryResultsDir;
	
	/* This helps track the nuner of control messages coming in */
	private Map<Integer, Integer> superCubeNumNodesMap;
	
	/* Keeping track of how many data messages are coming from a neighbor node */
	private Map<String, Integer> nodeToNumberOfDataMessagesMap;
	/* The values are strings like node+pathid */
	/* Keeping track of which paths are needed by a particular supercube */
	private Map<Integer, List<String>> supercubeToExpectedPathsMap;
	
	private Map<Integer, List<LocalRequirements>> supercubeToRequirementsMap;
	private Map<String, List<String>> pathIdToFragmentDataMap;
	private int numCores;
	private List<Integer> superCubeLocalFetchCheck;
	private List<String> resultFiles;
	private ExecutorService joinExecutors;
	private int cubesLeft;
	private int[] aPosns;
	private int[] bPosns;
	private double[] epsilons;
	private String hostName;
	private String port;
	private int interpolatingFeature;
	private MyPorter model;
	private boolean fixedBeta;
	

	public NeighborRequestHandler(List<NeighborDataEvent> internalEvents, List<NeighborDataEvent> individualRequests, Collection<NetworkDestination> destinations, EventContext clientContext,
			RequestListener listener, List<SuperCube> allCubes, Map<Integer, Integer> superCubeNumNodesMap, 
			int numCores, GeoavailabilityQuery geoQuery, GeospatialFileSystem fs1, String eventId, String queryResultsDir, 
			int[] aPosns, int[] bPosns, double[] epsilons, String hostName, String port, int interpolatingFeature2, boolean fixedBeta, String model) throws IOException {
		
		this.nodes = destinations;
		this.clientContext = clientContext;
		this.requestListener = listener;
		this.allCubes = allCubes;
		this.cubesLeft = allCubes.size();

		this.internalEvents = internalEvents;
		this.individualRequests = individualRequests;
		this.router = new ClientMessageRouter(true);
		this.router.addListener(this);
		this.responses = new ArrayList<GalileoMessage>();
		this.eventMap = new GalileoEventMap();
		this.eventWrapper = new BasicEventWrapper(this.eventMap);
		this.expectedControlMessages = new AtomicInteger(this.nodes.size());
		
		this.supercubeToExpectedPathsMap = Collections.synchronizedMap(new HashMap<Integer, List<String>>());
		this.nodeToNumberOfDataMessagesMap = new HashMap<String, Integer>();
		this.supercubeToRequirementsMap = new HashMap<Integer, List<LocalRequirements>>();
		this.numCores = numCores;
		this.geoQuery = geoQuery;
		this.fs1 = fs1;
		
		fs1SuperCubeDataMap = Collections.synchronizedMap(new HashMap<Integer, List<String[]>>());
		superCubeLocalFetchCheck = new ArrayList<Integer>();
		pathIdToFragmentDataMap = Collections.synchronizedMap(new HashMap<String, List<String>>());
		cleanupCandidateCubes = new ArrayList<Integer>();
		resultFiles = new ArrayList<String>();
		this.eventId = eventId;
		this.queryResultsDir = queryResultsDir;
		this.joinExecutors = Executors.newFixedThreadPool(Math.min(allCubes.size(), 2 * numCores));
		logger.info("RIKI: THREADS NUM: "+allCubes.size()+" "+numCores);
		this.superCubeNumNodesMap = superCubeNumNodesMap;
		this.aPosns = aPosns;
		this.bPosns =  bPosns;
		this.epsilons = epsilons;
		
		this.hostName = hostName;
		this.port = port;
		
		
		this.interpolatingFeature = interpolatingFeature2;
		this.fixedBeta = fixedBeta;
		if(!fixedBeta)
			this.model = new MyPorter(model);
		else
			this.model = null;
	}

	public void closeRequest() {
		
		silentClose(); // closing the router to make sure that no new responses
						// are added.

		if (this.response instanceof DataIntegrationResponse) {
			joinExecutors = null;
			logger.info("RIKI: THESE ARE THE FINAL PATHS BEING SENT OUT: "+resultFiles);
			DataIntegrationResponse actualResponse = (DataIntegrationResponse) this.response;
			actualResponse.setResultPaths(resultFiles);
			actualResponse.setNodeName(hostName);
			actualResponse.setNodePort(port);
			
		} 
		System.gc();
		this.requestListener.onRequestCompleted(this.response, clientContext, this);
	}
	
	/* CONTROL MESSAGE */
	private List<Requirements> handleRequirementsOnControlMessage (String requirementsString, int superCubeId, String nodeName) {
		
		String lines[] = requirementsString.trim().split("\\r?\\n");
		
		/* The requirements String of a particular supercube*/
		for(String line: lines) {
			String[] tokens = line.split("-"); 
			if(tokens.length == 2) {
				int pathIndex = -99;
				synchronized(supercubeToExpectedPathsMap) {
					
					List<String> pathsForThisCube = supercubeToExpectedPathsMap.get(superCubeId);
					if(pathsForThisCube == null) {
						pathsForThisCube = new ArrayList<String>();
					}
					
					pathIndex = Integer.valueOf(tokens[0]);
					pathsForThisCube.add(nodeName+"$"+pathIndex);
					supercubeToExpectedPathsMap.put(superCubeId, pathsForThisCube);
					
				}
				String[] fragments = tokens[1].split(",");
				LocalRequirements lr = createRequirement(pathIndex, fragments, nodeName);
				
				synchronized(supercubeToRequirementsMap) {
					List<LocalRequirements> lrs = supercubeToRequirementsMap.get(superCubeId);
					if(lrs == null) {
						lrs = new ArrayList<LocalRequirements>();
					}
					lrs.add(lr);
					supercubeToRequirementsMap.put(superCubeId, lrs);
				}
				
				
			} else {
				logger.log(Level.SEVERE, "AN INVALID FORMAT FOUND IN CONTROL MESSAGE "+lines[0]);
				return null;
			}
		}
		return null;
		
	}
	
	
	private LocalRequirements createRequirement(int pathIndex, String[] fragments, String nodeName) {
		
		LocalRequirements lr = new LocalRequirements(pathIndex,fragments, false, nodeName);
		return lr;
	}
	

	@Override
	public void onMessage(GalileoMessage message) {
		
		try{
			Event event = this.eventWrapper.unwrap(message);
			
			if(event instanceof NeighborDataResponse) {
				
				NeighborDataResponse rsp = (NeighborDataResponse)event;
				boolean isControlMessage = checkMessageType(rsp);
				
				/* This is a node telling beforehand what is coming */
				// CONTROL MESSAGE
				//logger.info("RIKI: SUPERCUBES TO NUMNODESMAP: "+ superCubeNumNodesMap);
				if(isControlMessage) {
					
					/* The machine this response came from */
					String nodeName = rsp.getNodeString();
					logger.log(Level.INFO, "RIKI : CONTROL MESSAGE RECEIVED FROM "+ nodeName 
							+ " FOR SUPERCUBES: " + rsp.getSupercubeIDList()+ " EXPECTING TOTAL PATHS: "+rsp.getTotalPaths());
					
					/* This node is about to send back this many data messages */
					if(rsp.getTotalPaths() > 0) {
						synchronized(nodeToNumberOfDataMessagesMap) {
							nodeToNumberOfDataMessagesMap.put(nodeName, rsp.getTotalPaths());
						}
					}
					
					if(rsp.getSupercubeIDList() != null && rsp.getSupercubeIDList().size() > 0) {
						/* The # of supercubes returns =  number of requirements string */
						for(int superCubeId: rsp.getSupercubeIDList()) {
							int index = rsp.getSupercubeIDList().indexOf(superCubeId);
							
							int expectedNodes = 0;
							/* This supercube is expecting control message from one less nodes */
							synchronized(superCubeNumNodesMap) {
								expectedNodes = superCubeNumNodesMap.get(superCubeId) - 1;
								
								/* One less node to expect control message from */
								superCubeNumNodesMap.put(superCubeId, expectedNodes);
							}
							
							/*Requirements for a single cube came back as 
							 * pathIndex-0,1,2...\n */
							if(rsp.getRequirementsList() != null && rsp.getRequirementsList().size() > 0) {
								String requirementsString = rsp.getRequirementsList().get(index);
								//logger.log(Level.INFO, "RIKI : RETURNED REQUIREMENTS STRING LOOKS LIKE: "+nodeName +" "+requirementsString);
								handleRequirementsOnControlMessage(requirementsString, superCubeId, nodeName);
							}
							
							// If all control messages have been received and still no expected paths
							// supercubeToExpectedPathsMap is for data messages
							if(expectedNodes == 0) {
								synchronized(supercubeToExpectedPathsMap) {
									List<String> rlist = supercubeToExpectedPathsMap.get(superCubeId);
									if(rlist ==null || rlist.size() <= 0 ) {
										cubesLeft--;
										logger.log(Level.INFO,"RIKI: REMOVED A CUBE FOR NO PATHS: "+superCubeId +" LEFT: "+cubesLeft);
										
										if(cubesLeft == 0) {
											
											// LAUNCH CLOSE REQUEST
											logger.log(Level.INFO, "All Joins on this node finished and saved");
											
											pathIdToFragmentDataMap = null;
											superCubeLocalFetchCheck = null;
											fs1SuperCubeDataMap = null;
											
											new Thread() {
												public void run() {
													NeighborRequestHandler.this.closeRequest();
												}
											}.start();
										}
									}
								}
							}
						}
					}
					
					/* Update supercube requirements */
				} else {
					/* Update path fragments available */
					/* TCP SHOULD ENSURE THAT THE CONTROL MESSAGES ARRIVE BEFORE DATA MESSAGES */
					/* DATA MESSAGE INCOMING */
					
					List<String> fragmentedRecords = rsp.getResultRecordLists();
					
					
					String nodeName = rsp.getNodeString();
					int pathIndex = rsp.getPathIndex();
					
			
					String pathString = nodeName+"$"+pathIndex;
					logger.log(Level.INFO, "RIKI : DATA MESSAGE RECEIVED FROM "+pathString + " "+System.currentTimeMillis());
					
					// Taking the fragments in a path to fragments map 
					synchronized(pathIdToFragmentDataMap) {
						//System.out.println("RIKI: WHAT CAME BACK: "+ fragmentedRecords);
						pathIdToFragmentDataMap.put(pathString, fragmentedRecords);
						
					}
					
					// Checking if a control message has been received from this node
					boolean noControl = checkForDataBeforeControlMsg(nodeName);
					
					if(noControl)
						logger.log(Level.INFO, "RIKI : NO CONTROL MESSAGE HAS COME IN FOR NODE" + nodeName);
					
					// We assume the control message from this node has already been received and processed
					synchronized(supercubeToExpectedPathsMap) {
						List<Integer> removalsI = new ArrayList<Integer>();
						
						for(int i : supercubeToExpectedPathsMap.keySet()) {
							
							List<String> expectedPaths = supercubeToExpectedPathsMap.get(i);
							//logger.log(Level.INFO, "RIKI : EXPECTED PATHS " + expectedPaths);
							//logger.log(Level.INFO, "RIKI : RECEIVED PATH " + pathString);
							if(expectedPaths.contains(pathString)) {
								
								// checking if a local fetch on this supercube has finished
								boolean localFetchDone = false;
								
								synchronized (superCubeLocalFetchCheck) {
									localFetchDone = checkForLocalFetch(i);
								}
								
								synchronized(cleanupCandidateCubes) {
									if(!localFetchDone) {
										// means localfetch for this node has not been done yet
										// This supercube has to be shelved and checked later on using the cleanup service
										if(!cleanupCandidateCubes.contains(i))
											cleanupCandidateCubes.add(i);
										
										CleanupService cs = new CleanupService();
										cs.run();
										
									}
								}
								
								expectedPaths.remove(pathString);
								//logger.log(Level.INFO, "RIKI: SUPERCUBE " + i +" NEEDS "+expectedPaths.size() +" MORE DATA MESSAGES");
								
								if(!localFetchDone)
									continue;
								
								// Whether all control messages have come in for this cube?
								int numNodesPendingControlMessages = 0;
								
								synchronized(superCubeNumNodesMap) {
									numNodesPendingControlMessages = superCubeNumNodesMap.get(i);
								}
								// This supercube has everything it needs
								if(expectedPaths.size() <= 0 && numNodesPendingControlMessages == 0) {
									removalsI.add(i);
									/* LAUNCH THIS SUPERCUBE INTO A NEW THREAD*/
									logger.log(Level.INFO, "RIKI: READY TO LAUNCH SUPERCUBE" + i);
									
									//JoiningThread jt = new JoiningThread(fs1SuperCubeDataMap.get(i), bRecords, aPosns, bPosns, epsilons, cubeId);
									
									//I commented
									JoiningThread jt = new JoiningThread(i);
									joinExecutors.execute(jt);
									
									// executor shutdown could be done in closeRequest()
								}
							}
						}
						
						if(!removalsI.isEmpty()) {
							for(int i : removalsI) {
								logger.info("RIKI: REMOVING SUPERCUBE: " + i);
								supercubeToExpectedPathsMap.remove(i);
							}
							logger.info("RIKI: AFTER REMOVAL: " + supercubeToExpectedPathsMap.keySet());
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
	
	private boolean checkForLocalFetch(int i) {
		// TODO Auto-generated method stub
		boolean found = false;
		
		
		found = superCubeLocalFetchCheck.contains(i);
		
		return found;
	}

	/**
	 * @param pathString
	 * @return
	 * 
	 * checks if a data message has arrived before control message.
	 * The supercube in question needs to be stored for bookkeeping
	 */
	private boolean checkForDataBeforeControlMsg(String nodeName) {
		// TODO Auto-generated method stub
		boolean hasNoControl = false;
		synchronized(nodeToNumberOfDataMessagesMap) {
			if(nodeToNumberOfDataMessagesMap.get(nodeName) == null)
				hasNoControl = true;
		}
		return hasNoControl;
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
			logger.info("RIKI: SENT OUT DATA MOVEMENT REQUEST AT: " + System.currentTimeMillis());
			this.response = response;
			
			/* size of individualRequests must be the same as nodes */
			int count = 0;
			
			// Reading in all fs1 blocks first
			
			LocalReader lr = new LocalReader();
			lr.run();
			//readFS1Blocks(allCubes);
			
			/* TODO: HANDLE INTERNAL EVENTS BEFORE SENDING OUT REQUESTS */
			logger.info("RIKI: TOTAL NUMBER OF SUPERCUBES " + cubesLeft + " LIST: "+ allCubes);
			for (NetworkDestination node : nodes) {
				Event request = individualRequests.get(count);
				GalileoMessage mrequest = this.eventWrapper.wrap(request);
				//this.router.sendMessage(node, mrequest);
				logger.info("Request sent to " + node.toString());
				count++;
			}
				
			this.elapsedTime = System.currentTimeMillis();
			// My code
			try {
				Thread.sleep(3000);
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			logger.log(Level.INFO,"DONE SLEEPING");
			new Thread() {
				public void run() {
					NeighborRequestHandler.this.closeRequest();
				}
			}.start();
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
					
					//logger.info("RIKI: CG" + sc.getCentralGeohash()+ " "+ geoQuery.getPolygon());
					Bitmap queryBitmap = null;
					
					if (geoQuery.getPolygon() != null) {
						queryBitmap = QueryTransform.queryToGridBitmap(geoQuery, blockGrid);
						
						/*if(queryBitmap == null) {
							
							continue;
						}*/
					}
					
					
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
						
							if (qp.getResultRecordLists() != null && qp.getResultRecordLists().size() > 0) {
								//logger.log(Level.INFO, "RIKI : ENTERED VALUES "+ qp.getResultRecordLists() +" FOR "+qp.getSuperCubeId());
								
								fs1SuperCubeDataMap.put(qp.getSuperCubeId(), qp.getResultRecordLists());
							} else {
								fs1SuperCubeDataMap.put(qp.getSuperCubeId(), null);
							}
							superCubeLocalFetchCheck.add(qp.getSuperCubeId());
						}
					}
				}
				
				logger.info("FINISHED READING LOCALLY...");
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Something went wrong while querying FS1 at NeighborRequestHandler", e);
			}
		}
	}
	
	class CleanupService implements Runnable {
		
		@Override
		public void run() {
			
			while(cubesLeft > 0 || !cleanupCandidateCubes.isEmpty()) {
				synchronized(cleanupCandidateCubes) {
					for(int c : cleanupCandidateCubes) {
						// Checking if a local fetch on this cube has occurred
						boolean localFetch = false;
						
						synchronized (superCubeLocalFetchCheck) {
							localFetch = checkForLocalFetch(c);
						}
						// local fetch has now finished
						if(localFetch) {
							synchronized(supercubeToExpectedPathsMap) {
								List<String> expectedPaths = supercubeToExpectedPathsMap.get(c);
								
								synchronized(pathIdToFragmentDataMap) {
									
									Set<String> allPaths = pathIdToFragmentDataMap.keySet();
									
									// THIS IS THE SOURCE OF CONCURRENT MODIFICATION EXCEPTION
									// ITERATING AND DELETING FROM THE EXPECTEDPATHS IN TEH SAME LOOP
									if(!Collections.disjoint(allPaths, expectedPaths)) {
										
										List<String> newExpectedPaths = new ArrayList<String>();
										boolean updated = false;
										for(String ePath: expectedPaths) {
											//String ePath = iterator0.next();
											//Iterator<String> iterator = pathIdToFragmentDataMap.keySet().iterator();
											
											if(allPaths != null && allPaths.contains(ePath)) {
												expectedPaths.remove(ePath);
												updated = true;
												continue;
											}
											newExpectedPaths.add(ePath);
											
										}
										
										if(updated) {
											supercubeToExpectedPathsMap.put(c, newExpectedPaths);
										}
										
										
									}
									
								}
								
								if(supercubeToExpectedPathsMap.get(c).isEmpty()) {
									// This cube is ready to be launched
									int indx = cleanupCandidateCubes.indexOf(c);
									cleanupCandidateCubes.remove(indx);
									
									//I commented
									JoiningThread jt = new JoiningThread(c);
									joinExecutors.execute(jt);
									
									
									if(cubesLeft == 1) {
										logger.info("RIKI: ETA HOYCHE");
										joinExecutors.shutdown();
										boolean status;
										try {
											status = joinExecutors.awaitTermination(10, TimeUnit.MINUTES);
											if (!status)
												logger.log(Level.WARNING, "Executor terminated because of the specified timeout=2minutes");
										} catch (InterruptedException e1) {
											// TODO Auto-generated catch block
											e1.printStackTrace();
										}
									}
									
									
								}
							}
						}
					}
				}
				
			}
			
		}
		
	}
	
	
	public static void main(String arg[]) {
		
		GeoavailabilityGrid blockGrid = new GeoavailabilityGrid("9y", GeoHash.MAX_PRECISION * 2 / 3);
		
		Coordinates c1 = new Coordinates(45.17086f,-113.0457f);
		Coordinates c2 = new Coordinates(45.17086f,-100.0457f);
		Coordinates c3 = new Coordinates(39.37086f,-100.0457f);
		Coordinates c4 = new Coordinates(39.37086f,-113.0457f);
		//Coordinates c5 = new Coordinates(36.78f, -107.64f);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1); cl.add(c2); cl.add(c3); cl.add(c4);
		
		GeoavailabilityQuery gq = new GeoavailabilityQuery(cl);
		Bitmap queryBitmap = null;
		
		if (gq.getPolygon() != null)
			queryBitmap = QueryTransform.queryToGridBitmap(gq, blockGrid);
		
		
	} 
	
	class JoiningThread implements Runnable {
		List<String[]> indvARecords;
		String bRecords;
		int cubeId;
		/*int[] aPosns; 
		int[] bPosns; 
		double[] epsilons;*/
		String storagePath;
		List<Integer> allFragsTo26 = new ArrayList<Integer>();
		
		/*public JoiningThread(List<String[]> indvARecords,String bRecords, int[] aPosns, int[] bPosns, double[] epsilons, int cubeId) {
			// TODO Auto-generated constructor stub
			this.indvARecords = indvARecords;
			this.bRecords = bRecords;
			this.aPosns = aPosns;
			this.bPosns = bPosns;
			this.epsilons = epsilons;
			this.storagePath = getResultFilePrefix(eventId, fs1.getName(), cubeId);
			
		}*/
		public JoiningThread(int i) {
			
			allFragsTo26.add(0);allFragsTo26.add(1);allFragsTo26.add(2);allFragsTo26.add(3);allFragsTo26.add(4);allFragsTo26.add(5);allFragsTo26.add(6);
			allFragsTo26.add(7);allFragsTo26.add(8);allFragsTo26.add(9);allFragsTo26.add(10);allFragsTo26.add(11);allFragsTo26.add(12);allFragsTo26.add(13);
			allFragsTo26.add(14);allFragsTo26.add(15);allFragsTo26.add(16);allFragsTo26.add(17);allFragsTo26.add(18);allFragsTo26.add(19);allFragsTo26.add(20);
			allFragsTo26.add(21);allFragsTo26.add(22);allFragsTo26.add(23);allFragsTo26.add(24);allFragsTo26.add(25);allFragsTo26.add(26);
			
			synchronized(fs1SuperCubeDataMap) {
				//logger.log(Level.INFO, "RIKI: FS1 RECORDS LOCAL: "+Arrays.asList(fs1SuperCubeDataMap.get(i)));
				this.indvARecords = fs1SuperCubeDataMap.get(i);
			}
			synchronized(supercubeToRequirementsMap) {
				List<LocalRequirements> listReq = supercubeToRequirementsMap.get(i);
				
				String bRecords = "";
				StringBuffer sb = new StringBuffer();
				
				for(LocalRequirements lr: listReq) {
					int pi = lr.getPathIndex();
					String key = lr.getNodeName()+"$"+pi;
					// This is coming from control message
					List<Integer> frags = lr.getFragments();
					
					//System.out.println("LOCAL REQUIREMENTS FOR SUPERCUBE: "+key+" "+frags);
					
					synchronized(pathIdToFragmentDataMap) {
						List<String> allFrags = pathIdToFragmentDataMap.get(key);
						
						if(allFrags == null || allFrags.size() == 0)
							continue;
						
						if(frags.containsAll(allFragsTo26) && allFrags.get(27) != null && !allFrags.get(27).isEmpty()) {
							frags = new ArrayList<Integer>();
							frags.add(27);
							
						}
						
						//System.out.println("EXISTING FRAGMENTS FOR PATH : "+key+" "+allFrags);
						int cnt = 0;
						
						
						for(int frag: frags) {
							
							String frg = allFrags.get(frag);
							/*if(cnt == frags.size() - 1)
								continue;*/
							if(frg.length() > 2) {
								//bRecords += allFrags.get(frag);
								
								//bRecords+="\n";
								sb.append(allFrags.get(frag)+"\n");
							}
							cnt++;
						}
					}
					
				}
				
				bRecords = sb.toString();
				this.cubeId = i;
				this.bRecords = bRecords;
				
				/*String aas = "";
				for(String[] aa : indvARecords) {
					aas+= Arrays.asList(aa)+"\n";
				}*/
				//logger.log(Level.INFO, "RIKI: FS2 RECORDS: "+bRecords.length());
				//logger.log(Level.INFO, "RIKI: AFS1 RECORDS: "+indvARecords.size());
				
				this.storagePath = getResultFilePrefix(eventId, fs1.getName(), i);
			}
		}
		@Override
		public void run() {
			logger.info("RIKI: BEFORE JOIN RUN FOR CUBE "+cubeId);
			MDC m = new MDC();
			List<String> joinRes = new ArrayList<String>();
			if(indvARecords!= null && indvARecords.size() > 0 && bRecords.length() > 0)
				joinRes = m.iterativeMultiDimJoin(indvARecords, bRecords, aPosns, bPosns, epsilons, interpolatingFeature);
			// TODO Auto-generated method stub
			logger.info("RIKI: AFTER JOIN RUN FOR CUBE "+cubeId);
			
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
			indvARecords = null;
			bRecords = null;
			
			logger.info("RIKI: AFTER SAVE FOR CUBE "+cubeId+" "+storagePath);
			synchronized(resultFiles) {
				if(storagePath != null)
					resultFiles.add(storagePath);
			}
			
			cubesLeft--;
			
			//logger.info("RIKI: NUMBER OF CUBES LEFT: "+cubesLeft + " "+allCubes);
			if(cubesLeft == 0) {
				pathIdToFragmentDataMap = null;
				superCubeLocalFetchCheck = null;
				fs1SuperCubeDataMap = null;
				
				// LAUNCH CLOSE REQUEST
				logger.log(Level.INFO, "All Joins on this node finished and saved");
				new Thread() {
					public void run() {
						NeighborRequestHandler.this.closeRequest();
					}
				}.start();
			} else if(cubesLeft < 0) {
				
				logger.info("RIKI: NEGATIVE CUBES LEFT");
			}
			
		}

	}
	
	private String getResultFilePrefix(String queryId, String fsName, int cubeIdentifier) {
		return this.queryResultsDir + "/" + String.format("%s-%s-%s", fsName, queryId, cubeIdentifier);
	}
	
	
	
	
	
}
