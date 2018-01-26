/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
 */

package galileo.dht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.bmp.Bitmap;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityQuery;
import galileo.bmp.QueryTransform;
import galileo.comm.BlockRequest;
import galileo.comm.BlockResponse;
import galileo.comm.DataIntegrationEvent;
import galileo.comm.DataIntegrationRequest;
import galileo.comm.DataIntegrationResponse;
import galileo.comm.FilesystemAction;
import galileo.comm.FilesystemEvent;
import galileo.comm.FilesystemRequest;
import galileo.comm.GalileoEventMap;
import galileo.comm.MetadataEvent;
import galileo.comm.MetadataRequest;
import galileo.comm.MetadataResponse;
import galileo.comm.NeighborDataEvent;
import galileo.comm.NeighborDataResponse;
import galileo.comm.QueryEvent;
import galileo.comm.QueryRequest;
import galileo.comm.QueryResponse;
import galileo.comm.StorageEvent;
import galileo.comm.StorageRequest;
import galileo.comm.TemporalType;
import galileo.config.SystemConfig;
import galileo.dataset.Block;
import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.TemporalHash;
import galileo.event.Event;
import galileo.event.EventContext;
import galileo.event.EventHandler;
import galileo.event.EventReactor;
import galileo.fs.FileSystemException;
import galileo.fs.GeospatialFileSystem;
import galileo.graph.Path;
import galileo.net.ClientConnectionPool;
import galileo.net.MessageListener;
import galileo.net.NetworkDestination;
import galileo.net.PortTester;
import galileo.net.RequestListener;
import galileo.net.ServerMessageRouter;
import galileo.query.Query;
import galileo.serialization.SerializationException;
import galileo.util.BorderingProperties;
import galileo.util.GeoHash;
import galileo.util.PathFragments;
import galileo.util.PathsAndOrientations;
import galileo.util.Requirements;
import galileo.util.SuperCube;
import galileo.util.SuperPolygon;
import galileo.util.Version;

/**
 * Primary communication component in the Galileo DHT. StorageNodes service
 * client requests and communication from other StorageNodes to disseminate
 * state information throughout the DHT.
 *
 * @author malensek
 */
public class StorageNode implements RequestListener {

	private static final Logger logger = Logger.getLogger("galileo");
	private StatusLine nodeStatus;

	private String hostname; // The name of this host
	private String canonicalHostname; // The fqdn of this host
	private int port;
	private String rootDir;
	private String resultsDir;
	private int numCores;

	private File pidFile;
	private File fsFile;

	private NetworkInfo network;

	private ServerMessageRouter messageRouter;
	private ClientConnectionPool connectionPool;
	private Map<String, GeospatialFileSystem> fsMap;

	private GalileoEventMap eventMap = new GalileoEventMap();
	private EventReactor eventReactor = new EventReactor(this, eventMap);
	private List<ClientRequestHandler> requestHandlers;
	/* My addition */
	private List<NeighborRequestHandler> rikiHandlers;

	private ConcurrentHashMap<String, QueryTracker> queryTrackers = new ConcurrentHashMap<>();

	// private String sessionId;

	public StorageNode() throws IOException {
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
			this.canonicalHostname = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			this.hostname = System.getenv("HOSTNAME");
			if (hostname == null || hostname.length() == 0)
				throw new UnknownHostException(
						"Failed to identify host name of the storage node. Details follow: " + e.getMessage());
		}
		this.hostname = this.hostname.toLowerCase();
		this.canonicalHostname = this.canonicalHostname.toLowerCase();
		this.port = NetworkConfig.DEFAULT_PORT;
		SystemConfig.reload();
		this.rootDir = SystemConfig.getRootDir();
		this.resultsDir = this.rootDir + "/.results";
		this.nodeStatus = new StatusLine(SystemConfig.getRootDir() + "/status.txt");
		this.fsFile = new File(SystemConfig.getRootDir() + "/storage-node.fs");
		if (!this.fsFile.exists())
			this.fsFile.createNewFile();
		String pid = System.getProperty("pidFile");
		if (pid != null) {
			this.pidFile = new File(pid);
		}
		this.numCores = Runtime.getRuntime().availableProcessors();
		this.requestHandlers = new CopyOnWriteArrayList<ClientRequestHandler>();
		this.rikiHandlers = new CopyOnWriteArrayList<NeighborRequestHandler>();
	}

	/**
	 * Begins Server execution. This method attempts to fail fast to provide
	 * immediate feedback to wrapper scripts or other user interface tools. Only
	 * once all the prerequisite components are initialized and in a sane state
	 * will the StorageNode begin accepting connections.
	 */
	public void start() throws Exception {
		Version.printSplash();

		/* First, make sure the port we're binding to is available. */
		nodeStatus.set("Attempting to bind to port");
		if (PortTester.portAvailable(port) == false) {
			nodeStatus.set("Could not bind to port " + port + ".");
			throw new IOException("Could not bind to port " + port);
		}

		/*
		 * Read the network configuration; if this is invalid, there is no need
		 * to execute the rest of this method.
		 */
		nodeStatus.set("Reading network configuration");
		network = NetworkConfig.readNetworkDescription(SystemConfig.getNetworkConfDir());

		// identifying the group of this storage node
		boolean nodeFound = false;
		for (NodeInfo node : network.getAllNodes()) {
			String nodeName = node.getHostname();
			if (nodeName.equals(this.hostname) || nodeName.equals(this.canonicalHostname)) {
				nodeFound = true;
				break;
			}
		}
		if (!nodeFound)
			throw new Exception("Failed to identify the group of the storage node. "
					+ "Type 'hostname' in the terminal and make sure that it matches the "
					+ "hostnames specified in the network configuration files.");

		nodeStatus.set("Restoring filesystems");
		File resultsDir = new File(this.resultsDir);
		if (!resultsDir.exists())
			resultsDir.mkdirs();

		this.fsMap = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(fsFile))) {
			String jsonSource = br.readLine();
			if (jsonSource != null && jsonSource.length() > 0) {
				JSONObject fsJSON = new JSONObject(jsonSource);
				for (String fsName : JSONObject.getNames(fsJSON)) {
					try {
						GeospatialFileSystem gfs = GeospatialFileSystem.restoreState(this, network,
								fsJSON.getJSONObject(fsName));
						this.fsMap.put(fsName, gfs);
						logger.info("Successfully restored the filesystem - " + fsName);
					} catch (Exception e) {
						logger.log(Level.SEVERE, "could not restore filesystem - " + fsName, e);
					}
				}
			}
		} catch (IOException ioe) {
			logger.log(Level.SEVERE, "Failed to restore filesystems", ioe);
		}

		/* Set up our Shutdown hook */
		Runtime.getRuntime().addShutdownHook(new ShutdownHandler());

		/* Pre-scheduler setup tasks */
		connectionPool = new ClientConnectionPool();
		connectionPool.addListener(eventReactor);

		/* Start listening for incoming messages. */
		messageRouter = new ServerMessageRouter();
		messageRouter.addListener(eventReactor);
		messageRouter.listen(port);
		nodeStatus.set("Online");

		/* Start processing the message loop */
		while (true) {
			try {
				eventReactor.processNextEvent();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "An exception occurred while processing next event. "
						+ "Storage node is still up and running. Exception details follow:", e);
			}
		}
	}

	private void sendEvent(NodeInfo node, Event event) throws IOException {
		connectionPool.sendMessage(node, eventReactor.wrapEvent(event));
	}

	@EventHandler
	public void handleFileSystemRequest(FilesystemRequest request, EventContext context)
			throws HashException, IOException, PartitionException {
		
		logger.info("RECEIVED CREATE FS REQUEST");
		String name = request.getName();
		FilesystemAction action = request.getAction();
		List<NodeInfo> nodes = network.getAllNodes();
		FilesystemEvent event = new FilesystemEvent(name, action, request.getFeatureList(), request.getSpatialHint());
		event.setPrecision(request.getPrecision());
		event.setNodesPerGroup(request.getNodesPerGroup());
		event.setTemporalType(request.getTemporalType());
		event.setRasterized(request.isRasterized());
		event.setSpatialUncertaintyPrecision(request.getSpatialUncertaintyPrecision());
		event.setTemporalUncertaintyPrecision(request.getTemporalUncertaintyPrecision());
		event.setTemporalHint(request.getTemporalHint());
		for (NodeInfo node : nodes) {
			logger.info("Requesting " + node + " to perform a file system action");
			sendEvent(node, event);
		}
	}

	@EventHandler
	public void handleFileSystem(FilesystemEvent event, EventContext context) {
		logger.info("FORWARDED CREATE FS REQUEST");
		logger.log(Level.INFO,
				"Performing action " + event.getAction().getAction() + " for file system " + event.getName());
		if (event.getAction() == FilesystemAction.CREATE) {
			GeospatialFileSystem fs = fsMap.get(event.getName());
			if (fs == null) {
				try {
					fs = new GeospatialFileSystem(this, this.rootDir, event.getName(), event.getPrecision(),
							event.getNodesPerGroup(), event.getTemporalValue(), this.network, event.getFeatures(),
							event.getSpatialHint(),event.getTemporalHint(), false, event.getSpatialUncertaintyPrecision(), event.getTemporalUncertaintyPrecision(), event.isRasterized());
					fsMap.put(event.getName(), fs);
				} catch (FileSystemException | SerializationException | IOException | PartitionException | HashException
						| HashTopologyException e) {
					logger.log(Level.SEVERE, "Could not initialize the Galileo File System!", e);
				}
			}
		} else if (event.getAction() == FilesystemAction.DELETE) {
			GeospatialFileSystem fs = fsMap.get(event.getName());
			if (fs != null) {
				fs.shutdown();
				fsMap.remove(event.getName());
				java.nio.file.Path directory = Paths.get(rootDir + File.separator + event.getName());
				try {
					Files.walkFileTree(directory, new SimpleFileVisitor<java.nio.file.Path>() {
						@Override
						public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
								throws IOException {
							Files.delete(file);
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException exc)
								throws IOException {
							Files.delete(dir);
							return FileVisitResult.CONTINUE;
						}
					});
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Failed to delete the requested file System!", e);
				}
			}
		}
		persistFilesystems();
	}

	/**
	 * Handles a storage request from a client. This involves determining where
	 * the data belongs via a {@link Partitioner} implementation and then
	 * forwarding the data on to its destination.
	 */
	@EventHandler
	public void handleStorageRequest(StorageRequest request, EventContext context)
			throws HashException, IOException, PartitionException {
		/* Determine where this block goes. */
		Block file = request.getBlock();
		String fsName = file.getFilesystem();
		if (fsName != null) {
			GeospatialFileSystem gfs = this.fsMap.get(fsName);
			if (gfs != null) {
				Metadata metadata = file.getMetadata();
				Partitioner<Metadata> partitioner = gfs.getPartitioner();
				NodeInfo node = partitioner.locateData(metadata);
				logger.log(Level.INFO, "Storage destination: {0}", node);
				StorageEvent store = new StorageEvent(file);
				sendEvent(node, store);
			} else {
				logger.log(Level.WARNING, "No filesystem found for the specified name " + fsName + ". Request ignored");
			}
		} else {
			logger.log(Level.WARNING, "No filesystem name specified to store the block. Request ignored");
		}
	}

	@EventHandler
	public void handleStorage(StorageEvent store, EventContext context) {
		String fsName = store.getBlock().getFilesystem();
		GeospatialFileSystem fs = fsMap.get(fsName);
		if (fs != null) {
			logger.log(Level.INFO, "Storing block " + store.getBlock() + " to filesystem " + fsName);
			try {
				fs.storeBlock(store.getBlock());
			} catch (FileSystemException | IOException e) {
				logger.log(Level.SEVERE, "Something went wrong while storing the block.", e);
			}
		} else {
			logger.log(Level.SEVERE, "Requested file system(" + fsName + ") not found. Ignoring the block.");
		}
	}
	
	
	private class ParallelReader implements Runnable {
		private Block block;
		private GeospatialFileSystem gfs;
		private String blockPath;
		
		public ParallelReader(GeospatialFileSystem gfs, String blockPath){
			this.gfs = gfs;
			this.blockPath = blockPath;
		}
		
		public Block getBlock(){
			return this.block;
		}
		
		@Override
		public void run(){
			try {
				this.block = gfs.retrieveBlock(blockPath);
				if(blockPath.startsWith(resultsDir))
					new File(blockPath).delete();
			} catch (IOException | SerializationException e) {
				logger.log(Level.SEVERE, "Failed to retrieve the block", e);
			}
		}
	}
	

	@EventHandler
	public void handleBlockRequest(BlockRequest blockRequest, EventContext context) {
		String fsName = blockRequest.getFilesystem();
		GeospatialFileSystem fs = fsMap.get(fsName);
		List<Block> blocks = new ArrayList<Block>();
		if (fs != null) {
			logger.log(Level.FINE, "Retrieving blocks " + blockRequest.getFilePaths() + " from filesystem " + fsName);
			try {
				List<String> blockPaths = blockRequest.getFilePaths();
				if(blockPaths.size() > 1){
					ExecutorService executor = Executors.newFixedThreadPool(Math.min(blockPaths.size(), 2 * numCores));
					List<ParallelReader> readers = new ArrayList<>();
					for(String blockPath : blockPaths){
						ParallelReader pr = new ParallelReader(fs, blockPath);
						readers.add(pr);
						executor.execute(pr);
					}
					executor.shutdown();
					executor.awaitTermination(10, TimeUnit.MINUTES);
					for(ParallelReader reader : readers)
						if(reader.getBlock() != null)
							blocks.add(reader.getBlock());
				} else {
					ParallelReader pr = new ParallelReader(fs, blockPaths.get(0));
					pr.run();
					blocks.add(pr.getBlock());
				}
				context.sendReply(new BlockResponse(blocks.toArray(new Block[blocks.size()])));
			} catch (IOException | InterruptedException e) {
				logger.log(Level.SEVERE, "Something went wrong while retrieving the block.", e);
				try {
					context.sendReply(new BlockResponse(new Block[]{}));
				} catch (IOException e1) {
					logger.log(Level.SEVERE, "Failed to send response to the original client", e1);
				}
			}
		} else {
			logger.log(Level.SEVERE, "Requested file system(" + fsName + ") not found. Returning empty content.");
			try {
				context.sendReply(new BlockResponse(new Block[]{}));
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Failed to send response to the original client", e);
			}
		}
	}

	/**
	 * Handles a meta request that seeks information regarding the galileo
	 * system.
	 */
	@EventHandler
	public void handleMetadataRequest(MetadataRequest request, EventContext context) {
		try {
			logger.info("Meta Request: " + request.getRequest().getString("kind"));
			if ("galileo#filesystem".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#filesystem");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetadataEvent(request.getRequest()), new MetadataResponse(response));
				this.requestHandlers.add(reqHandler);
			} else if ("galileo#features".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#features");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetadataEvent(request.getRequest()), new MetadataResponse(response));
				this.requestHandlers.add(reqHandler);
			} else if ("galileo#overview".equalsIgnoreCase(request.getRequest().getString("kind"))) {
				JSONObject response = new JSONObject();
				response.put("kind", "galileo#overview");
				response.put("result", new JSONArray());
				ClientRequestHandler reqHandler = new ClientRequestHandler(network.getAllDestinations(), context, this);
				reqHandler.handleRequest(new MetadataEvent(request.getRequest()), new MetadataResponse(response));
				this.requestHandlers.add(reqHandler);
			} else {
				JSONObject response = new JSONObject();
				response.put("kind", request.getRequest().getString("kind"));
				response.put("error", "invalid request");
				context.sendReply(new MetadataResponse(response));
			}
		} catch (Exception e) {
			JSONObject response = new JSONObject();
			String kind = "unknown";
			if (request.getRequest().has("kind"))
				kind = request.getRequest().getString("kind");
			response.put("kind", kind);
			response.put("error", e.getMessage());
			try {
				context.sendReply(new MetadataResponse(response));
			} catch (IOException e1) {
				logger.log(Level.SEVERE, "Failed to send response to the original client", e);
			}
		}
	}

	@EventHandler
	public void handleMetadata(MetadataEvent event, EventContext context) throws IOException {
		if ("galileo#filesystem".equalsIgnoreCase(event.getRequest().getString("kind"))) {
			JSONObject response = new JSONObject();
			response.put("kind", "galileo#filesystem");
			JSONArray result = new JSONArray();
			for (String fsName : fsMap.keySet()) {
				GeospatialFileSystem fs = fsMap.get(fsName);
				result.put(fs.obtainState());
			}
			response.put("result", result);
			context.sendReply(new MetadataResponse(response));
		} else if ("galileo#overview".equalsIgnoreCase(event.getRequest().getString("kind"))) {
			JSONObject request = event.getRequest();
			JSONObject response = new JSONObject();
			response.put("kind", "galileo#overview");
			JSONArray result = new JSONArray();
			if (request.has("filesystem") && request.get("filesystem") instanceof JSONArray) {
				JSONArray fsNames = request.getJSONArray("filesystem");
				for (int i = 0; i < fsNames.length(); i++) {
					GeospatialFileSystem fs = fsMap.get(fsNames.getString(i));
					if (fs != null) {
						JSONArray overview = fs.getOverview();
						JSONObject fsOverview = new JSONObject();
						fsOverview.put(fsNames.getString(i), overview);
						result.put(fsOverview);
					} else {
						JSONObject fsOverview = new JSONObject();
						fsOverview.put(fsNames.getString(i), new JSONArray());
						result.put(fsOverview);
					}
				}
			}
			response.put("result", result);
			logger.info(response.toString());
			context.sendReply(new MetadataResponse(response));
		} else if ("galileo#features".equalsIgnoreCase(event.getRequest().getString("kind"))) {
			JSONObject request = event.getRequest();
			JSONObject response = new JSONObject();
			response.put("kind", "galileo#features");
			JSONArray result = new JSONArray();
			if (request.has("filesystem") && request.get("filesystem") instanceof JSONArray) {
				JSONArray fsNames = request.getJSONArray("filesystem");
				for (int i = 0; i < fsNames.length(); i++) {
					GeospatialFileSystem fs = fsMap.get(fsNames.getString(i));
					if (fs != null) {
						JSONArray features = fs.getFeaturesJSON();
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsNames.getString(i), features);
						result.put(fsFeatures);
					} else {
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsNames.getString(i), new JSONArray());
						result.put(fsFeatures);
					}
				}
			} else {
				for (String fsName : fsMap.keySet()) {
					GeospatialFileSystem fs = fsMap.get(fsName);
					if (fs != null) {
						JSONArray features = fs.getFeaturesJSON();
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsName, features);
						result.put(fsFeatures);
					} else {
						JSONObject fsFeatures = new JSONObject();
						fsFeatures.put(fsName, new JSONArray());
						result.put(fsFeatures);
					}
				}
			}
			response.put("result", result);
			context.sendReply(new MetadataResponse(response));
		} else {
			JSONObject response = new JSONObject();
			response.put("kind", event.getRequest().getString("kind"));
			response.put("result", new JSONArray());
			context.sendReply(new MetadataResponse(response));
		}
	}

	/**
	 * Handles a query request from a client. Query requests result in a number
	 * of subqueries being performed across the Galileo network.
	 * 
	 * @throws PartitionException
	 * @throws HashException
	 */
	@EventHandler
	public void handleQueryRequest(QueryRequest request, EventContext context) {
		String featureQueryString = request.getFeatureQueryString();
		String metadataQueryString = request.getMetadataQueryString();
		logger.log(Level.INFO, "Feature query request: {0}", featureQueryString);
		logger.log(Level.INFO, "Metadata query request: {0}", metadataQueryString);
		String queryId = String.valueOf(System.currentTimeMillis());
		GeospatialFileSystem gfs = this.fsMap.get(request.getFilesystemName());
		
		/* POPULATING THE METADATA FOR THE QUERY */
		if (gfs != null) {
			QueryResponse response = new QueryResponse(queryId, gfs.getFeaturesRepresentation(), new JSONObject());
			
			/* This metadata is simply used to find nodes. 
			 * It only contains temnporal and spatial features */
			Metadata data = new Metadata();
			if (request.isTemporal()) {
				
				/* Time in request if a - separated string */
				String[] timeSplit = request.getTime().split("-");
				int timeIndex = Arrays.asList(TemporalType.values()).indexOf(gfs.getTemporalType());
				if (!timeSplit[timeIndex].contains("x")) {
					logger.log(Level.INFO, "Temporal query: {0}", request.getTime());
					Calendar c = Calendar.getInstance();
					c.setTimeZone(TemporalHash.TIMEZONE);
					int year = timeSplit[0].charAt(0) == 'x' ? c.get(Calendar.YEAR) : Integer.parseInt(timeSplit[0]);
					int month = timeSplit[1].charAt(0) == 'x' ? c.get(Calendar.MONTH)
							: Integer.parseInt(timeSplit[1]) - 1;
					int day = timeSplit[2].charAt(0) == 'x' ? c.get(Calendar.DAY_OF_MONTH)
							: Integer.parseInt(timeSplit[2]);
					int hour = timeSplit[3].charAt(0) == 'x' ? c.get(Calendar.HOUR_OF_DAY)
							: Integer.parseInt(timeSplit[3]);
					c.set(year, month, day, hour, 0);
					data.setTemporalProperties(new TemporalProperties(c.getTimeInMillis()));
				}
			}
			if (request.isSpatial()) {
				logger.log(Level.INFO, "Spatial query: {0}", request.getPolygon());
				data.setSpatialProperties(new SpatialProperties(new SpatialRange(request.getPolygon())));
			}
			Partitioner<Metadata> partitioner = gfs.getPartitioner();
			List<NodeInfo> nodes;
			try {
				/* TemporalHierarchyPartitioner */
				/* ===================Finding the nodes that satisfy the query================== */
				nodes = partitioner.findDestinations(data);
				logger.info("destinations: " + nodes);
				QueryEvent qEvent = (request.hasFeatureQuery() || request.hasMetadataQuery())
						? new QueryEvent(queryId, request.getFilesystemName(), request.getFeatureQuery(),
								request.getMetadataQuery())
						: (request.isSpatial())
								? new QueryEvent(queryId, request.getFilesystemName(), request.getPolygon())
								: new QueryEvent(queryId, request.getFilesystemName(), request.getTime());

				if (request.isDryRun()) {
					qEvent.enableDryRun();
					response.setDryRun(true);
				}
				if (request.isSpatial())
					qEvent.setPolygon(request.getPolygon());
				if (request.isTemporal())
					qEvent.setTime(request.getTime());

				try {
					ClientRequestHandler reqHandler = new ClientRequestHandler(new ArrayList<NetworkDestination>(nodes),
							context, this);
					
					/* Sending out query to all nodes */
					reqHandler.handleRequest(qEvent, response);
					this.requestHandlers.add(reqHandler);
				} catch (IOException ioe) {
					logger.log(Level.SEVERE,
							"Failed to initialize a ClientRequestHandler. Sending unfinished response back to client",
							ioe);
					try {
						context.sendReply(response);
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Failed to send response back to original client", e);
					}
				}
			} catch (HashException | PartitionException hepe) {
				logger.log(Level.SEVERE,
						"Failed to identify the destination nodes. Sending unfinished response back to client", hepe);
				try {
					context.sendReply(response);
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Failed to send response back to original client", e);
				}
			}
		} else {
			try {
				QueryResponse response = new QueryResponse(queryId, new JSONArray(), new JSONObject());
				context.sendReply(response);
			} catch (IOException ioe) {
				logger.log(Level.SEVERE, "Failed to send response back to original client", ioe);
			}
		}
	}

	private String getResultFilePrefix(String queryId, String fsName, String blockIdentifier) {
		return this.resultsDir + "/" + String.format("%s-%s-%s", fsName, queryId, blockIdentifier);
	}

	private class QueryProcessor implements Runnable {
		private String blockPath;
		private String pathPrefix;
		private GeoavailabilityQuery geoQuery;
		private GeoavailabilityGrid grid;
		private GeospatialFileSystem gfs;
		private Bitmap queryBitmap;
		private List<String> resultPaths;
		private long fileSize;

		public QueryProcessor(GeospatialFileSystem gfs, String blockPath, GeoavailabilityQuery gQuery,
				GeoavailabilityGrid grid, Bitmap queryBitmap, String pathPrefix) {
			this.gfs = gfs;
			this.blockPath = blockPath;
			this.geoQuery = gQuery;
			this.grid = grid;
			this.queryBitmap = queryBitmap;
			this.pathPrefix = pathPrefix;
		}

		@Override
		public void run() {
			try {
				this.resultPaths = this.gfs.query(this.blockPath, this.geoQuery, this.grid, this.queryBitmap,
						this.pathPrefix);
				for (String resultPath : this.resultPaths)
					this.fileSize += new File(resultPath).length();
			} catch (IOException | InterruptedException e) {
				logger.log(Level.SEVERE, "Something went wrong while querying the filesystem. No results obtained.");
			}
		}

		public long getFileSize() {
			return this.fileSize;
		}

		public List<String> getResultPaths() {
			return this.resultPaths;
		}
	}

	/**
	 * Handles an internal Query request (from another StorageNode)
	 */
	@EventHandler
	public void handleQuery(QueryEvent event, EventContext context) {
		long hostFileSize = 0;
		long totalProcessingTime = 0;
		long blocksProcessed = 0;
		int totalNumPaths = 0;
		JSONArray header = new JSONArray();
		JSONObject blocksJSON = new JSONObject();
		JSONArray resultsJSON = new JSONArray();
		long processingTime = System.currentTimeMillis();
		try {
			logger.info(event.getFeatureQueryString());
			logger.info(event.getMetadataQueryString());
			String fsName = event.getFilesystemName();
			GeospatialFileSystem fs = fsMap.get(fsName);
			if (fs != null) {
				header = fs.getFeaturesRepresentation();
				/* Feature Query is not needed to list blocks */
				Map<String, List<String>> blockMap = fs.listBlocks(event.getTime(), event.getPolygon(),
						event.getMetadataQuery(), event.isDryRun());
				if (event.isDryRun()) {
					/*
					 * TODO: Make result of dryRun resemble the format of that
					 * of non-dry-run so that the end user can retrieve the
					 * blocks from the block paths
					 **/
					JSONObject responseJSON = new JSONObject();
					responseJSON.put("filesystem", event.getFilesystemName());
					responseJSON.put("queryId", event.getQueryId());
					for (String blockKey : blockMap.keySet()) {
						blocksJSON.put(blockKey, new JSONArray(blockMap.get(blockKey)));
					}
					responseJSON.put("result", blocksJSON);
					QueryResponse response = new QueryResponse(event.getQueryId(), header, responseJSON);
					response.setDryRun(true);
					context.sendReply(response);
					return;
				}
				JSONArray filePaths = new JSONArray();
				int totalBlocks = 0;
				for (String blockKey : blockMap.keySet()) {
					List<String> blocks = blockMap.get(blockKey);
					totalBlocks += blocks.size();
					for(String block : blocks){
						filePaths.put(block);
						hostFileSize += new File(block).length();
					}
				}
				if (totalBlocks > 0) {
					if (event.getFeatureQuery() != null || event.getPolygon() != null) {
						hostFileSize = 0;
						filePaths = new JSONArray();
						// maximum parallelism = 64
						ExecutorService executor = Executors.newFixedThreadPool(Math.min(totalBlocks, 2 * numCores));
						List<QueryProcessor> queryProcessors = new ArrayList<>();
						GeoavailabilityQuery geoQuery = new GeoavailabilityQuery(event.getFeatureQuery(),
								event.getPolygon());
						for (String blockKey : blockMap.keySet()) {
							
							/* Converts the bounds of geohash into a 1024x1024 region */
							GeoavailabilityGrid blockGrid = new GeoavailabilityGrid(blockKey,
									GeoHash.MAX_PRECISION * 2 / 3);
							Bitmap queryBitmap = null;
							if (geoQuery.getPolygon() != null)
								queryBitmap = QueryTransform.queryToGridBitmap(geoQuery, blockGrid);
							List<String> blocks = blockMap.get(blockKey);
							for (String blockPath : blocks) {
								QueryProcessor qp = new QueryProcessor(fs, blockPath, geoQuery, blockGrid, queryBitmap,
										getResultFilePrefix(event.getQueryId(), fsName, blockKey + blocksProcessed));
								blocksProcessed++;
								queryProcessors.add(qp);
								executor.execute(qp);
							}
						}
						executor.shutdown();
						boolean status = executor.awaitTermination(10, TimeUnit.MINUTES);
						if (!status)
							logger.log(Level.WARNING, "Executor terminated because of the specified timeout=10minutes");
						for (QueryProcessor qp : queryProcessors) {
							if (qp.getFileSize() > 0) {
								hostFileSize += qp.getFileSize();
								for (String resultPath : qp.getResultPaths())
									filePaths.put(resultPath);
							}
						}
					} 
				}
				totalProcessingTime = System.currentTimeMillis() - processingTime;
				totalNumPaths = filePaths.length();
				JSONObject resultJSON = new JSONObject();
				resultJSON.put("filePath", filePaths);
				resultJSON.put("numPaths", totalNumPaths);
				resultJSON.put("fileSize", hostFileSize);
				resultJSON.put("hostName", this.canonicalHostname);
				resultJSON.put("hostPort", this.port);
				resultJSON.put("processingTime", totalProcessingTime);
				resultsJSON.put(resultJSON);
			} else {
				logger.log(Level.SEVERE, "Requested file system(" + fsName
						+ ") not found. Ignoring the query and returning empty results.");
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"Something went wrong while querying the filesystem. No results obtained. Sending blank list to the client. Issue details follow:",
					e);
		}

		JSONObject responseJSON = new JSONObject();
		responseJSON.put("filesystem", event.getFilesystemName());
		responseJSON.put("queryId", event.getQueryId());
		if (hostFileSize == 0) {
			responseJSON.put("result", new JSONArray());
			responseJSON.put("hostFileSize", new JSONObject());
			responseJSON.put("totalFileSize", 0);
			responseJSON.put("totalNumPaths", 0);
			responseJSON.put("hostProcessingTime", new JSONObject());
		} else {
			responseJSON.put("result", resultsJSON);
			responseJSON.put("hostFileSize", new JSONObject().put(this.canonicalHostname, hostFileSize));
			responseJSON.put("totalFileSize", hostFileSize);
			responseJSON.put("totalNumPaths", totalNumPaths);
			responseJSON.put("hostProcessingTime", new JSONObject().put(this.canonicalHostname, totalProcessingTime));
		}
		responseJSON.put("totalProcessingTime", totalProcessingTime);
		responseJSON.put("totalBlocksProcessed", blocksProcessed);
		QueryResponse response = new QueryResponse(event.getQueryId(), header, responseJSON);
		try {
			context.sendReply(response);
		} catch (IOException ioe) {
			logger.log(Level.SEVERE, "Failed to send response back to ClientRequestHandler", ioe);
		}
	}

	@EventHandler
	public void handleQueryResponse(QueryResponse response, EventContext context) throws IOException {
		QueryTracker tracker = queryTrackers.get(response.getId());
		if (tracker == null) {
			logger.log(Level.WARNING, "Unknown query response received: {0}", response.getId());
			return;
		}
	}

	/**
	 * Triggered when the request is completed by the
	 * {@link ClientRequestHandler}
	 */
	@Override
	public void onRequestCompleted(Event response, EventContext context, MessageListener requestHandler) {
		try {
			logger.info("Sending collective response to the client");
			if(requestHandler instanceof ClientRequestHandler) {
				this.requestHandlers.remove(requestHandler);
			} else if (requestHandler instanceof NeighborRequestHandler) {
				this.rikiHandlers.remove(requestHandler);
			}
			
			context.sendReply(response);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed to send response to the client.", e);
		} finally {
			System.gc();
		}
	}

	public void persistFilesystems() {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(fsFile))) {
			JSONObject fsJSON = new JSONObject();
			for (String fsName : fsMap.keySet()) {
				GeospatialFileSystem fs = fsMap.get(fsName);
				fsJSON.put(fsName, fs.obtainState());
			}
			bw.write(fsJSON.toString());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Handles cleaning up the system for a graceful shutdown.
	 */
	private class ShutdownHandler extends Thread {
		@Override
		public void run() {
			/*
			 * The logging subsystem may have already shut down, so we revert to
			 * stdout for our final messages
			 */
			System.out.println("Initiated shutdown.");

			try {
				connectionPool.forceShutdown();
				messageRouter.shutdown();
			} catch (Exception e) {
				e.printStackTrace();
			}

			nodeStatus.close();

			if (pidFile != null && pidFile.exists()) {
				pidFile.delete();
			}

			persistFilesystems();

			for (GeospatialFileSystem fs : fsMap.values())
				fs.shutdown();

			System.out.println("Goodbye!");
		}
	}
	
	/**
	 * Look into request, find what nodes to query based on space, time values.
	 * Pass on request to respective nodes. 
	 * @author sapmitra
	 * @param request
	 * @param context
	 */
	@EventHandler
	public void handleDataIntegrationRequest(DataIntegrationRequest request, EventContext context) {
		
		String featureQueryString = request.getFeatureQueryString();
		logger.log(Level.INFO, "Feature query request: {0}", featureQueryString);
		
		String eventId = String.valueOf(System.currentTimeMillis());
		
		GeospatialFileSystem gfs;
		
		gfs = this.fsMap.get(request.getFsname1());
		
		if(gfs != null) {
			
			DataIntegrationResponse response = new DataIntegrationResponse();
			
			Metadata data = getQueryMetadata(request, gfs);
			
			Partitioner<Metadata> partitioner = gfs.getPartitioner();
			List<NodeInfo> nodes;
			
			try {
				/* ===================Finding the nodes that satisfy the query (for primary FS)================== */
				
				/* TemporalHierarchyPartitioner */
				nodes = partitioner.findDestinations(data);
				
				logger.info("Destinations for FS1 DataIntegrationRequest: " + nodes);
				
				DataIntegrationEvent dintEvent = createDataIntegrationEvent(request);
				try {
					ClientRequestHandler reqHandler = new ClientRequestHandler(new ArrayList<NetworkDestination>(nodes),
							context, this);
					
					/* Sending out query to all nodes */
					reqHandler.handleRequest(dintEvent, response);
					this.requestHandlers.add(reqHandler);
					
				} catch (IOException ioe) {
					logger.log(Level.SEVERE,
							"Failed to initialize a ClientRequestHandler. Sending unfinished response back to client",
							ioe);
					try {
						context.sendReply(response);
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Failed to send response back to original client", e);
					}
				}
			} catch (HashException | PartitionException hepe) {
				logger.log(Level.SEVERE,
						"Failed to identify the destination nodes. Sending unfinished response back to client", hepe);
				try {
					context.sendReply(response);
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Failed to send response back to original client", e);
				}
			}
		}
		
		
	}
	
	/**
	 * @author sapmitra
	 * @param request
	 * @return
	 */
	private DataIntegrationEvent createDataIntegrationEvent(DataIntegrationRequest request) {
		DataIntegrationEvent dintEvent = new DataIntegrationEvent();
		
		if(request.hasFeatureQuery()) {
			dintEvent.setFeatureQuery(request.getFeatureQuery());
		}
		
		if (request.isSpatial())
			dintEvent.setPolygon(request.getPolygon());
		if (request.isTemporal())
			dintEvent.setTime(request.getTime());
		
		dintEvent.setTimeRelaxation(request.getTimeRelaxation());
		dintEvent.setSpaceRelaxation(request.getSpaceRelaxation());
		dintEvent.setFsname1(request.getFsname1());
		dintEvent.setFsname2(request.getFsname2());
		dintEvent.setPrimaryFS(request.getPrimaryFS());

		return dintEvent;
	}

	/**
	 * @author sapmitra
	 * @param request
	 * @param gfs
	 * @return
	 * 
	 * Creating a metadata object out of the request's temporal and spatial conditions
	 */
	private Metadata getQueryMetadata(DataIntegrationRequest request,GeospatialFileSystem gfs) {
		Metadata data = new Metadata();
		if (request.isTemporal()) {
			
			/* Time in request if a '-' separated string */
			
			String[] timeSplit = request.getTime().split("-");
			
			int timeIndex = Arrays.asList(TemporalType.values()).indexOf(gfs.getTemporalType());
			if (!timeSplit[timeIndex].contains("x")) {
				logger.log(Level.INFO, "Temporal query: {0}", request.getTime());
				
				Calendar c = Calendar.getInstance();
				c.setTimeZone(TemporalHash.TIMEZONE);
				
				int year = timeSplit[0].charAt(0) == 'x' ? c.get(Calendar.YEAR) : Integer.parseInt(timeSplit[0]);
				int month = timeSplit[1].charAt(0) == 'x' ? c.get(Calendar.MONTH) : Integer.parseInt(timeSplit[1]) - 1;
				int day = timeSplit[2].charAt(0) == 'x' ? c.get(Calendar.DAY_OF_MONTH) : Integer.parseInt(timeSplit[2]);
				int hour = timeSplit[3].charAt(0) == 'x' ? c.get(Calendar.HOUR_OF_DAY) : Integer.parseInt(timeSplit[3]);
				c.set(year, month, day, hour, 0);
				data.setTemporalProperties(new TemporalProperties(c.getTimeInMillis()));
			}
		}
		if (request.isSpatial()) {
			logger.log(Level.INFO, "Spatial query: {0}", request.getPolygon());
			data.setSpatialProperties(new SpatialProperties(new SpatialRange(request.getPolygon())));
		}
		
		return data;
	}
	
	/**
	 * @author sapmitra
	 * @param request
	 * @param context
	 */
	@EventHandler
	public void handleDataIntegration(DataIntegrationEvent event, EventContext context) {
		logger.log(Level.INFO, "RECEIVED A FS1 DATAINTEGRATIONEVENT");
		// fs1 is the primary filesystem
		String fsName1 = event.getFsname1();
		GeospatialFileSystem fs1 = fsMap.get(fsName1);
		
		// fs2 is the secondary filesystem
		String fsName2 = event.getFsname2();
		GeospatialFileSystem fs2 = fsMap.get(fsName2);

		DataIntegrationResponse response = new DataIntegrationResponse();

		try {
			if (fs1 != null && fs2 != null) {

				// fs1 is the primary and fs2 is secondary filesystem

				// All blocks of fs1 on this node that match our criteria
				List<Path<Feature, String>> paths1 = fs1.listPaths(event.getTime(), event.getPolygon(), null, false);
				
				// LOGGING
				logger.log(Level.INFO, "PATHS FROM FS1:");
				for (Path<Feature, String> path : paths1) {
					List<String> blocks = new ArrayList<String>(path.getPayload());
					logger.log(Level.INFO, blocks.toString());
				}

				List<Coordinates> queryPolygon = event.getPolygon();
				List<Coordinates> superPolygon = SuperPolygon.getSuperPolygon(queryPolygon, fs2.getSpatialUncertaintyPrecision());
				
				logger.log(Level.INFO, "QUERY POLYGON :"+queryPolygon);
				logger.log(Level.INFO, "SUPER POLYGON :"+superPolygon);
				
				// For each block of fs1, we need to find what nodes to be queried in fs2
				// Also, we need to calculate the superblock that needs to be queried at each node

				// Map of what supercubes to query at each node (key->Supercube)
				Map<String, List<Integer>> nodeToCubeMap = new HashMap<String, List<Integer>>();
				List<NodeInfo> destinations = new ArrayList<NodeInfo>();
				List<SuperCube> allCubes = new ArrayList<SuperCube>();

				// Supercube index to all the nodes that this supercube has queried to
				Map<Integer, List<String>> supercubeToNodeMap = new HashMap<Integer, List<String>>();
				
				// This will come in handy while receiving neighbor responses from requested nodes.
				Map<Integer, Integer> superCubeNumNodesMap = new HashMap<Integer, Integer>();

				Partitioner<Metadata> partitioner = fs2.getPartitioner();
				Map<String, BorderingProperties> borderMap = fs1.getBorderMap();
				for (Path<Feature, String> path : paths1) {
					// All the blocks under a certain directory
					// There can be multiple blocks under the same directory as long as their names are different
					List<String> blocks = new ArrayList<String>(path.getPayload());

					// EXTRACT THE SUPERCUBE FOR EACH OF THE BLOCK RETURNED
					// Supercube also contains info about the core time and geoHash

					// This supercube is in terms of FS1 rules
					// Anything intersecting with this supercube in FS2 has to be returned

					SuperCube sc = extractSuperCube(path, fs1);
					sc.setFs1BlockPath(blocks);

					String cGeo = sc.getCentralGeohash();

					// Get the neighbors geohash, cgeo, that actually are needed, along with the centralGeohash
					String[] validNeighborsGeohash = GeoHash.checkForNeighborValidity(queryPolygon, fs1.getSpatialUncertaintyPrecision(), cGeo,borderMap, blocks);

					List<Date> dates = null;
					List<TemporalProperties> tprops = null;

					int indx = getMatchingCubeIndex(allCubes, sc);

					/* Adding this new supercube to the list */
					if (indx < 0) {
						indx = allCubes.size();
						sc.setId(indx);
						allCubes.add(sc);
					}

					SpatialProperties searchSp = new SpatialProperties(new SpatialRange(sc.getPolygon()));

					if (sc.getTime() != null) {
						/*
						 * since the time could span over multiple days, we need to check
						 */
						// getting all dates that may lie between two timestamps
						
						/* A string of two timestamps */
						String timeString = sc.getTime();
						
						boolean hasUp = false;
						boolean hasDown = false;
						for(String block : blocks) {
							
							BorderingProperties bpr = borderMap.get(block);
							if(hasUp && hasDown) {
								break;
							}
							if(bpr.getUpTimeEntries().size() > 0) {
								hasUp = true;
							}
							if(bpr.getDownTimeEntries().size() > 0) {
								hasDown = true;
							}
						}
						
						if(!hasUp || ! hasDown) {
							String[] tokens = sc.getCentralTime().split("-");
							String[] timestamps = timeString.split("-");
							if(!hasUp) {
								long end = GeoHash.getEndTimeStamp(tokens[0], tokens[1], tokens[2], tokens[3], fs1.getTemporalType());
								timestamps[1] = String.valueOf(end);
								
							}
							if(!hasDown) {
								
								long start = GeoHash.getStartTimeStamp(tokens[0], tokens[1], tokens[2], tokens[3], fs1.getTemporalType());
								timestamps[0] = String.valueOf(start);
								
							}
							timeString = timestamps[0] + "-" + timestamps[1];
						}
						
						
								
						dates = new ArrayList<Date>();
						dates = SuperCube.handleTemporalRangeForEachBlock(timeString);

						tprops = new ArrayList<TemporalProperties>();

						for (Date d : dates) {

							tprops.add(new TemporalProperties(d.getTime()));
						}
					}

					// Found what nodes to query based on supercube data
					// These are all nodes needed to query to get fs2 data required for this particular supercube in fs1
					List<NodeInfo> nodes = partitioner.findDestinationsForFS2(searchSp, tprops, fs2.getGeohashPrecision(), validNeighborsGeohash);

					// Hashcode calculation for NodeInfo is based on src and port
					// Getting unique should not be a problem later
					destinations.addAll(nodes);

					// Need to send out the same supercube to all destination nodes

					for (NodeInfo n : nodes) {

						String key = n.getHostname() + "-" + n.getPort();

						// Check if this cube is already getting queried at this
						// node
						if (!hasCube(nodeToCubeMap.get(key), indx)) {

							List<Integer> cubeIndices;
							if (nodeToCubeMap.get(key) == null) {
								cubeIndices = new ArrayList<Integer>();
							} else {
								cubeIndices = nodeToCubeMap.get(key);
							}

							cubeIndices.add(indx);
							nodeToCubeMap.put(key, cubeIndices);

							SuperCube.addToCubeNodeMap(supercubeToNodeMap, superCubeNumNodesMap, key, indx);

						}

					}

				}
				// send consolidated request for blocks to each of those nodes

				Set<NodeInfo> setNodes = new TreeSet<NodeInfo>(destinations);
				destinations = new ArrayList<NodeInfo>(setNodes);

				List<NeighborDataEvent> individualRequests = new ArrayList<NeighborDataEvent>();
				//List<NeighborDataEvent> internalEvents = new ArrayList<NeighborDataEvent>();

				// Before sending out requests to each node, catch the one
				// directed to this node and save it for later.
				for (NodeInfo n : destinations) {
					
					// See if this is the current node or not
					//boolean thisNode = checkForThisNode(n);

					String nodeKey = n.getHostname() + "-" + n.getPort();
					List<Integer> cubeIndices = nodeToCubeMap.get(nodeKey);
					
					/*int uncertaintyPrecision = fs1.getTemporalUncertaintyPrecision() > fs2.getTemporalUncertaintyPrecision() ? 
							fs2.getTemporalUncertaintyPrecision() : fs1.getTemporalUncertaintyPrecision();*/
					
					NeighborDataEvent nEvent = createNeighborRequestPerNode(cubeIndices, allCubes, fsName2,fsName1, superPolygon, event.getTime(), event.getFeatureQuery());

					/*if (thisNode) {
						internalEvents.add(nEvent);
						destinations.remove(n);
						continue;
					}*/

					individualRequests.add(nEvent);
				}
				
				GeoavailabilityQuery geoQuery = new GeoavailabilityQuery(event.getFeatureQuery(),
						event.getPolygon());
				
				NeighborRequestHandler rikiHandler = new NeighborRequestHandler(null, individualRequests, new ArrayList<NetworkDestination>(destinations), context, this,
						allCubes, superCubeNumNodesMap, numCores, geoQuery, fs1);
				rikiHandler.handleRequest(response);

			}

		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"Something went wrong while data integration event on the filesystem. No results obtained. Sending blank list to the client. Issue details follow:",
					e);
		}

	}
	
	private boolean checkForThisNode(NodeInfo n) {
		
		String hname = n.getHostname();
		int port = n.getPort();
		
		if(this.hostname.equals(hname) && this.port == port){
			return true;
		}
		
		return false;
	}
	
	/**
	 * The queryTime getting sent in NeighbotDataEvent is a string with two longs separated by -
	 * @author sapmitra
	 * @param cubeIndices
	 * @param allCubes
	 * @param reqFs
	 * @param superPolygon
	 * @param queryTime
	 * @param uncertaintyPrecision 
	 * @return
	 * @throws ParseException 
	 */
	private NeighborDataEvent createNeighborRequestPerNode(List<Integer> cubeIndices, List<SuperCube> allCubes, String reqFs,String srcFs, List<Coordinates> superPolygon, String queryTime, Query featureQuery) throws ParseException {
		List<SuperCube> toSend = new ArrayList<SuperCube>();
		
		NeighborDataEvent ne = null;
		
		for(int i: cubeIndices) {
			
			toSend.add(allCubes.get(i));
			
		}
		
		/*if(toSend.size() > 0) {
			
			String[] tokens = queryTime.split("-");
			
			TemporalType tt = getTemporalType(tokens);
			
			long start = GeoHash.getStartTimeStamp(tokens[0], tokens[1], tokens[2], tokens[3], tt) - uncertaintyPrecision;
			long end = GeoHash.getEndTimeStamp(tokens[0], tokens[1], tokens[2], tokens[3], tt) + uncertaintyPrecision;
			
			ne = new NeighborDataEvent(toSend, reqFs, superPolygon, start+"-"+end);
		}*/
		ne = new NeighborDataEvent(toSend, reqFs, srcFs, superPolygon, queryTime, featureQuery);
		
		return ne;
	}
	
	
	private NeighborDataResponse createCubeRequirements(Map<SuperCube, List<Requirements>> supercubeRequirementsMap, int totalPaths, String nodeString) {
		
		NeighborDataResponse rsp = new NeighborDataResponse(supercubeRequirementsMap, totalPaths, nodeString);
		return rsp;
	}
	
	/**
	 * This is a node returning FS2 data to a node with FS1 data that needs it.
	 * @author sapmitra
	 * @param event
	 * @param context
	 */
	@EventHandler
	public void handleNeighborData(NeighborDataEvent event, EventContext context) {
		
		String nodeString = hostname + "-" + port;
		
		// reqFs is the file-system for which we need the neighbor data i.e fs2
		String reqfsName = event.getReqFs();
		String srcfsName = event.getSrcFs();
		List<SuperCube> superCubes = event.getSupercubes();
		List<Coordinates> superPolygon = event.getSuperPolygon();
		
		GeospatialFileSystem reqFSystem = fsMap.get(reqfsName);
		GeospatialFileSystem srcFSystem = fsMap.get(srcfsName);
		
		//Partitioner<Metadata> fsPartitioner = reqFSystem.getPartitioner();
		try{
			PathsAndOrientations pao = reqFSystem.listIntersectingPathsWithOrientation(superCubes, superPolygon, event.getQueryTime(), null, srcFSystem.getTemporalType());
			
			if(pao != null) {
				
				/* Now to actually reading in the blocks */
				int totalBlocks = pao.getTotalBlocks();
				
				List<Path<Feature, String>> paths = pao.getPaths();
				
				int totalPaths = paths.size();
				Map<Path<Feature, String>, PathFragments> pathToFragmentsMap = pao.getPathToFragmentsMap();
				
				// ALERTING THE REQUESTING NODE BEFOREHAND ABOUT INCOMING NEIGHBOR DATA
				Map<SuperCube, List<Requirements>> supercubeRequirementsMap = pao.getSupercubeRequirementsMap();
				
				/* SEND BACK SUPERCUBE TO REQUIREMENTS MAP IMMEDIATELY */
				/* THERE IS GOING TO BE ONE DATA RESPONSE PER PATH. SO THE NUMBER OF PATHS IS NECESSARY */
				
				NeighborDataResponse controlMessage = createCubeRequirements(supercubeRequirementsMap, pathToFragmentsMap.size(), nodeString);
				context.sendReply(controlMessage);
				
				
				ExecutorService executor = Executors.newFixedThreadPool(Math.min(totalPaths, 2 * numCores));
				List<NeighborDataQueryProcessor> queryProcessors = new ArrayList<NeighborDataQueryProcessor>();
				GeoavailabilityQuery geoQuery = new GeoavailabilityQuery(event.getFeatureQuery(),
						event.getSuperPolygon());
				
				/* One thread per path */
				/* Keep track of total number of paths, so that the source knows when all the paths have been received */
				for(Path<Feature, String> path: paths) {
					String geohash = GeospatialFileSystem.getPathInfo(path, 1);
					int pathIndex = paths.indexOf(path);
					/* Converts the bounds of geohash into a 1024x1024 region */
					GeoavailabilityGrid blockGrid = new GeoavailabilityGrid(geohash, GeoHash.MAX_PRECISION * 2 / 3);
					Bitmap queryBitmap = null;
					if (geoQuery.getPolygon() != null)
						queryBitmap = QueryTransform.queryToGridBitmap(geoQuery, blockGrid);
						
					/* The blocks need to be processed part by part */
					/* Returns a 28 part list for all fragmented records in a single path */
					NeighborDataQueryProcessor qp = new NeighborDataQueryProcessor(reqFSystem, path, geoQuery, blockGrid, queryBitmap, pathToFragmentsMap.get(path), context, pathIndex, nodeString);
					queryProcessors.add(qp);
					executor.execute(qp);
					
				}
				executor.shutdown();
				boolean status = executor.awaitTermination(10, TimeUnit.MINUTES);
				
				if (!status)
					logger.log(Level.WARNING, "handleNeighborData:Executor terminated because of the specified timeout=10minutes");
				
				
			} else {
				// NEED TO RETURN A BLANK RESPONSE
			}
			
			
		}catch (Exception e) {
			logger.log(Level.SEVERE,
					"Something went wrong while data integration event on the filesystem. No results obtained. Sending blank list to the client. Issue details follow:",
					e);
		}
		
		// For each supercube, find out what paths are needed
		// Check orientation
		// Supercube has central time and geohash, so finding neighbor wont be a problem
		
		
	}
	
	/**
	 * 
	 * @author sapmitra
	 * @param path
	 * @param resultRecordLists : a list of 28 lists
	 * @return
	 */
	private JSONObject getNeighborResultsInJSON(Path<Feature, String> path, List<List<String[]>> resultRecordLists) {
		
		
		for(List<String[]> records: resultRecordLists) {
			JSONArray recordsJSON = new JSONArray();
			if(records == null) {
				recordsJSON.put("");
			}
			for(String[] record: records) {
				
				List<String> myList = Arrays.asList(record);
				String recStr = myList.toString();
				recordsJSON.put(recStr.substring(1, recStr.length() - 1));
			}
		}
		
		
		return null;
	}
	
	private boolean hasCube(List<Integer> list, int scb) {
		if(list ==null){
			return false;
		}
		for(int i: list) {
			if(scb == i) {
				return true;
			}
		}
		
		return false;
	}
	
	private int getMatchingCubeIndex(List<SuperCube> list, SuperCube scb) {
		
		if(list == null || list.isEmpty()) {
			
			return -1;
		}
		
		for(SuperCube s: list) {
			
			if(s.getCentralGeohash() != null && s.getCentralGeohash().equals(scb.getCentralGeohash())) {
				
				if(s.getCentralTime() == null && scb.getCentralTime() == null) {
					return list.indexOf(s);
					
				} else if(s.getCentralTime() != null && scb.getCentralTime() != null && s.getCentralTime().equals(scb.getCentralTime())) {
					
					return list.indexOf(s);
					
				}
			}
		}
		
		
		return -1;
	}

	

	/**
	 * @author sapmitra
	 * @param path this is the FS1 path
	 * @param fs1
	 * @param fs2
	 * @return
	 * @throws ParseException 
	 */
	private SuperCube extractSuperCube(Path<Feature, String> path, GeospatialFileSystem fs1) throws ParseException {

		final String TEMPORAL_YEAR_FEATURE = "x__year__x";
		final String TEMPORAL_MONTH_FEATURE = "x__month__x";
		final String TEMPORAL_DAY_FEATURE = "x__day__x";
		final String TEMPORAL_HOUR_FEATURE = "x__hour__x";
		final String SPATIAL_FEATURE = "x__spatial__x";
		SuperCube s = null;
		
		
		/* Extracting the spatial and temporal feature of this block */
		if (null != path && path.hasPayload()) {
			List<Feature> labels = path.getLabels();
			String space = "";
			String year = "xxxx", month = "xx", day = "xx", hour = "xx";
			int allset = 0;
			for (Feature label : labels) {
				switch (label.getName().toLowerCase()) {
				case TEMPORAL_YEAR_FEATURE:
					year = label.getString();
					allset++;
					break;
				case TEMPORAL_MONTH_FEATURE:
					month = label.getString();
					allset++;
					break;
				case TEMPORAL_DAY_FEATURE:
					day = label.getString();
					allset++;
					break;
				case TEMPORAL_HOUR_FEATURE:
					hour = label.getString();
					allset++;
					break;
				case SPATIAL_FEATURE:
					space = label.getString();
					allset++;

					break;
				}
				if (allset == 5)
					break;
			}
			
			// Getting the bounding coordinates of the super geohash
			List<Coordinates> bounds = GeoHash.getSuperGeohashes(space, fs1.getSpatialUncertaintyPrecision());
			
			s = new SuperCube();
			s.setCentralTime(day+"-"+month+"-"+year+"-"+hour);
			s.setCentralGeohash(space);
			s.setPolygon(bounds);

			// The start and end time-stamp might come in handy during listBlocks
			if(!year.contains("x")) {
				long start = GeoHash.getStartTimeStamp(year, month, day, hour, fs1.getTemporalType()) - fs1.getTemporalUncertaintyPrecision();
				long end = GeoHash.getEndTimeStamp(year, month, day, hour, fs1.getTemporalType()) + fs1.getTemporalUncertaintyPrecision();
				
				s.setTime(start+"-"+end);
			} else {
				s.setTime(null);
			}
			
		}
		
		return s;
	}
	
	
	public static void main1(String arg[]) throws ParseException {
		
		GeoHash.getEndTimeStamp("2014", "12", "2", "04", TemporalType.YEAR);
		
	}
	
	/**
	 * @author sapmitra
	 * @param year
	 * @param month
	 * @param day
	 * @param hour
	 * @param mode temporal magnification level. 1 for day/hour, 2 for just month, 3 for year
	 * @return
	 *//*
	private long getStartTimeStamp1(String year, String month, String day, String hour, int mode) {
		
		int yearL = Integer.parseInt(year);
		int monthL = Integer.parseInt(month);
		int dayL = Integer.parseInt(day);
		
		int hourL = 0;
		if(!"xx".equals(hour))
			hourL = Integer.parseInt(year);
		
		// day missing
		if(mode == 2) {
			dayL = 1;
		} else if(mode == 3) {
			dayL = 1;
			monthL = 1;
		}
		
		Date d = new Date(yearL, monthL, dayL);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(d);
		calendar.setTimeZone(TemporalHash.TIMEZONE);
		
		calendar.set(Calendar.HOUR_OF_DAY, hourL);
	    calendar.set(Calendar.MINUTE, 0);
	    calendar.set(Calendar.SECOND, 0);
	    calendar.set(Calendar.MILLISECOND, 0);
	    
	    return calendar.getTimeInMillis();
		
	}*/
	/**
	 * Executable entrypoint for a Galileo DHT Storage Node
	 */
	public static void main(String[] args) {
		try {
			StorageNode node = new StorageNode();
			node.start();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not start StorageNode.", e);
		}
	}
}
