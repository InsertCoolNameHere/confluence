/*
Copyright (c) 2014, Colorado State University
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

package galileo.fs;

import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import galileo.bmp.Bitmap;
import galileo.bmp.BitmapException;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityMap;
import galileo.bmp.GeoavailabilityQuery;
import galileo.comm.TemporalType;
import galileo.dataset.Block;
import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.Point;
import galileo.dataset.SpatialHint;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.dataset.feature.FeatureType;
import galileo.dht.GroupInfo;
import galileo.dht.LocalParallelQueryProcessor;
import galileo.dht.NeighborDataParallelQueryProcessor;
import galileo.dht.NetworkInfo;
import galileo.dht.NodeInfo;
import galileo.dht.PartitionException;
import galileo.dht.Partitioner;
import galileo.dht.StorageNode;
import galileo.dht.TemporalHierarchyPartitioner;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.TemporalHash;
import galileo.graph.FeaturePath;
import galileo.graph.MetadataGraph;
import galileo.graph.Path;
import galileo.query.Expression;
import galileo.query.Operation;
import galileo.query.Operator;
import galileo.query.Query;
import galileo.serialization.SerializationException;
import galileo.serialization.Serializer;
import galileo.util.PathFragments;
import galileo.util.BorderingProperties;
import galileo.util.GeoHash;
import galileo.util.Math;
import galileo.util.OrientationManager;
import galileo.util.Pair;
import galileo.util.PathsAndOrientations;
import galileo.util.Requirements;
import galileo.util.SuperCube;

/**
 * Implements a {@link FileSystem} for Geospatial data. This file system manager
 * assumes that the information being stored has both space and time properties.
 * <p>
 * Relevant system properties include galileo.fs.GeospatialFileSystem.timeFormat
 * and galileo.fs.GeospatialFileSystem.geohashPrecision to modify how the
 * hierarchy is created.
 */
public class GeospatialFileSystem extends FileSystem {

	private static final Logger logger = Logger.getLogger("galileo");

	private static final String DEFAULT_TIME_FORMAT = "yyyy" + File.separator + "M" + File.separator + "d";
	private static final int DEFAULT_GEOHASH_PRECISION = 4;
	private static final int MIN_GRID_POINTS = 5000;
	private int numCores;

	private static final String pathStore = "metadata.paths";

	private NetworkInfo network;
	private Partitioner<Metadata> partitioner;
	private TemporalType temporalType;
	private int nodesPerGroup;
	/*
	 * Must be comma-separated name:type string where type is an int returned by
	 * FeatureType
	 */
	private List<Pair<String, FeatureType>> featureList;
	private SpatialHint spatialHint;
	private String temporalHint;
	private String storageRoot;

	private MetadataGraph metadataGraph;

	private PathJournal pathJournal;

	private SimpleDateFormat timeFormatter;
	private String timeFormat;
	private int geohashPrecision;
	private TemporalProperties latestTime;
	private TemporalProperties earliestTime;
	private String latestSpace;
	private String earliestSpace;
	private Set<String> geohashIndex;
	private int temporalPosn;
	private int spatialPosn1;
	private int spatialPosn2;

	private static final String TEMPORAL_YEAR_FEATURE = "x__year__x";
	private static final String TEMPORAL_MONTH_FEATURE = "x__month__x";
	private static final String TEMPORAL_DAY_FEATURE = "x__day__x";
	private static final String TEMPORAL_HOUR_FEATURE = "x__hour__x";
	private static final String SPATIAL_FEATURE = "x__spatial__x";
	private static final String SPATIAL_BORDER_FEATURE = "x__spatialborder__x";
	private static final String TEMPORAL_BORDER_FEATURE = "x__temporalborder__x";
	
	private Map<String, BorderingProperties> borderMap;
	
	private int spatialUncertaintyPrecision;
	private int temporalUncertaintyPrecision;
	
	private boolean isRasterized;
	

	public GeospatialFileSystem(StorageNode sn, String storageDirectory, String name, int precision, int nodesPerGroup,
			int temporalType, NetworkInfo networkInfo, String featureList, SpatialHint sHint, boolean ignoreIfPresent)
			throws FileSystemException, IOException, SerializationException, PartitionException, HashException,
			HashTopologyException {
		super(storageDirectory, name, ignoreIfPresent);

		this.nodesPerGroup = nodesPerGroup;
		this.geohashIndex = new HashSet<>();
		
		//logger.log(Level.INFO, "RIKI: GROUPS: "+networkInfo.getGroups());
		this.borderMap = new HashMap<String, BorderingProperties>();
		/* featurelist is a comma separated list of feature names: type(int) */
		if (featureList != null) {
			this.featureList = new ArrayList<>();
			for (String nameType : featureList.split(",")) {
				String[] pair = nameType.split(":");
				this.featureList
						.add(new Pair<String, FeatureType>(pair[0], FeatureType.fromInt(Integer.parseInt(pair[1]))));
			}
			/* Cannot modify featurelist anymore */
			this.featureList = Collections.unmodifiableList(this.featureList);
		}
		this.spatialHint = sHint;
		if (this.featureList != null && this.spatialHint == null)
			throw new IllegalArgumentException("Spatial hint is needed when feature list is provided");
		this.storageRoot = storageDirectory;
		this.temporalType = TemporalType.fromType(temporalType);
		this.numCores = Runtime.getRuntime().availableProcessors();
		//logger.log(Level.INFO, "RIKI: NETWORK3: "+networkInfo);
		if (nodesPerGroup <= 0) {
			this.network = new NetworkInfo();
			List<GroupInfo> groups = networkInfo.getGroups();
			TemporalHash th = new TemporalHash(this.temporalType);
			int maxGroups = th.maxValue().intValue();
			for (int i = 0; i < maxGroups; i++)
				this.network.addGroup(groups.get(i));
		} else {
			this.network = new NetworkInfo();
			GroupInfo groupInfo = null;
			List<NodeInfo> allNodes = networkInfo.getAllNodes();
			Collections.sort(allNodes);
			TemporalHash th = new TemporalHash(this.temporalType);
			int maxGroups = th.maxValue().intValue();
			for (int i = 0; i < allNodes.size(); i++) {
				if (this.network.getGroups().size() < maxGroups) {
					if (i % nodesPerGroup == 0) {
						groupInfo = new GroupInfo(String.valueOf(i / nodesPerGroup));
						groupInfo.addNode(allNodes.get(i));
						this.network.addGroup(groupInfo);
					} else {
						groupInfo.addNode(allNodes.get(i));
					}
				}
			}
		}

		/*
		 * TODO: Ask end user about the partitioning scheme. chronospatial or
		 * spatiotemporal. Accordingly, use TemporalHierarchyPartitioner or
		 * SpatialHierarchyPartitioner
		 **/
		this.partitioner = new TemporalHierarchyPartitioner(sn, this.network, this.temporalType.getType());

		this.timeFormat = System.getProperty("galileo.fs.GeospatialFileSystem.timeFormat", DEFAULT_TIME_FORMAT);
		int maxPrecision = GeoHash.MAX_PRECISION / 5;
		this.geohashPrecision = (precision < 0) ? DEFAULT_GEOHASH_PRECISION
				: (precision > maxPrecision) ? maxPrecision : precision;

		this.timeFormatter = new SimpleDateFormat();
		this.timeFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		this.timeFormatter.applyPattern(timeFormat);
		this.pathJournal = new PathJournal(this.storageDirectory + File.separator + pathStore);

		createMetadataGraph();
	}
	
	public GeospatialFileSystem(StorageNode sn, String storageDirectory, String name, int precision, int nodesPerGroup,
			int temporalType, NetworkInfo networkInfo, String featureList, SpatialHint sHint, String temporalHint, boolean ignoreIfPresent, int spatialUncertainty, int temporalUncertainty, boolean isRasterized)
			throws FileSystemException, IOException, SerializationException, PartitionException, HashException,
			HashTopologyException {
		super(storageDirectory, name, ignoreIfPresent);
		
		this.borderMap = new HashMap<String, BorderingProperties>();
		
		this.spatialUncertaintyPrecision = spatialUncertainty;
		this.temporalUncertaintyPrecision = temporalUncertainty;
		this.isRasterized = isRasterized;
		this.nodesPerGroup = nodesPerGroup;
		this.geohashIndex = new HashSet<>();
		
		/* featurelist is a comma separated list of feature names: type(int) */
		if (featureList != null) {
			this.featureList = new ArrayList<>();
			int count = 0;
			for (String nameType : featureList.split(",")) {
				String[] pair = nameType.split(":");
				String featureName = pair[0];
				
				if(featureName.equals(temporalHint)) {
					temporalPosn = count;
				} else if(featureName.equals(sHint.getLatitudeHint())) {
					spatialPosn1 = count;
				} else if(featureName.equals(sHint.getLongitudeHint())) {
					spatialPosn2 = count;
				}
				count++;
				
				this.featureList
						.add(new Pair<String, FeatureType>(pair[0], FeatureType.fromInt(Integer.parseInt(pair[1]))));
			}
			/* Cannot modify featurelist anymore */
			this.featureList = Collections.unmodifiableList(this.featureList);
		}
		this.spatialHint = sHint;
		this.temporalHint = temporalHint;
		if (this.featureList != null && this.spatialHint == null)
			throw new IllegalArgumentException("Spatial hint is needed when feature list is provided");
		this.storageRoot = storageDirectory;
		this.temporalType = TemporalType.fromType(temporalType);
		this.numCores = Runtime.getRuntime().availableProcessors();

		if (nodesPerGroup <= 0) 
			nodesPerGroup = networkInfo.getGroups().get(0).getSize();
		
		this.network = new NetworkInfo();
		GroupInfo groupInfo = null;
		List<NodeInfo> allNodes = networkInfo.getAllNodes();
		Collections.sort(allNodes);
		TemporalHash th = new TemporalHash(this.temporalType);
		int maxGroups = th.maxValue().intValue();
		for (int i = 0; i < allNodes.size(); i++) {
			if (this.network.getGroups().size() < maxGroups) {
				if (i % nodesPerGroup == 0) {
					groupInfo = new GroupInfo(String.valueOf(i / nodesPerGroup));
					groupInfo.addNode(allNodes.get(i));
					this.network.addGroup(groupInfo);
				} else {
					groupInfo.addNode(allNodes.get(i));
				}
			}
		}

		/*
		 * TODO: Ask end user about the partitioning scheme. chronospatial or
		 * spatiotemporal. Accordingly, use TemporalHierarchyPartitioner or
		 * SpatialHierarchyPartitioner
		 **/
		this.partitioner = new TemporalHierarchyPartitioner(sn, this.network, this.temporalType.getType());

		this.timeFormat = System.getProperty("galileo.fs.GeospatialFileSystem.timeFormat", DEFAULT_TIME_FORMAT);
		int maxPrecision = GeoHash.MAX_PRECISION / 5;
		this.geohashPrecision = (precision < 0) ? DEFAULT_GEOHASH_PRECISION
				: (precision > maxPrecision) ? maxPrecision : precision;

		this.timeFormatter = new SimpleDateFormat();
		this.timeFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		this.timeFormatter.applyPattern(timeFormat);
		this.pathJournal = new PathJournal(this.storageDirectory + File.separator + pathStore);

		createMetadataGraph();
	}

	public JSONArray getFeaturesRepresentation() {
		JSONArray features = new JSONArray();
		if (this.featureList != null) {
			for (Pair<String, FeatureType> pair : this.featureList)
				features.put(pair.a + ":" + pair.b.name());
		}
		return features;
	}

	public List<String> getFeaturesList() {
		List<String> features = new ArrayList<String>();
		if (this.featureList != null) {
			for (Pair<String, FeatureType> pair : this.featureList)
				features.add(pair.a + ":" + pair.b.name());
		}
		return features;
	}

	public NetworkInfo getNetwork() {
		return this.network;
	}

	public Partitioner<Metadata> getPartitioner() {
		return this.partitioner;
	}

	public TemporalType getTemporalType() {
		return this.temporalType;
	}

	/**
	 * Initializes the Metadata Graph, either from a successful recovery from
	 * the PathJournal, or by scanning all the {@link Block}s on disk.
	 */
	private void createMetadataGraph() throws IOException {
		metadataGraph = new MetadataGraph();

		/* Recover the path index from the PathJournal */
		List<FeaturePath<String>> graphPaths = new ArrayList<>();
		boolean recoveryOk = pathJournal.recover(graphPaths);
		pathJournal.start();

		if (recoveryOk == true) {
			for (FeaturePath<String> path : graphPaths) {
				try {
					metadataGraph.addPath(path);
				} catch (Exception e) {
					logger.log(Level.WARNING, "Failed to add path", e);
					recoveryOk = false;
					break;
				}
			}
		}

		if (recoveryOk == false) {
			logger.log(Level.SEVERE, "Failed to recover path journal!");
			pathJournal.erase();
			pathJournal.start();
			fullRecovery();
		}
	}

	public synchronized JSONObject obtainState() {
		JSONObject state = new JSONObject();
		state.put("name", this.name);
		state.put("storageRoot", this.storageRoot);
		state.put("precision", this.geohashPrecision);
		state.put("nodesPerGroup", this.nodesPerGroup);
		state.put("geohashIndex", this.geohashIndex);
		StringBuffer features = new StringBuffer();
		if (this.featureList != null) {
			for (Pair<String, FeatureType> pair : this.featureList)
				features.append(pair.a + ":" + pair.b.toInt() + ",");
			features.setLength(features.length() - 1);
		}
		state.put("featureList", this.featureList != null ? features.toString() : JSONObject.NULL);
		JSONObject spHint = null;
		if (this.spatialHint != null) {
			spHint = new JSONObject();
			spHint.put("latHint", this.spatialHint.getLatitudeHint());
			spHint.put("lngHint", this.spatialHint.getLongitudeHint());
		}
		state.put("spatialHint", spHint == null ? JSONObject.NULL : spHint);
		state.put("temporalType", this.temporalType.getType());
		state.put("temporalString", this.temporalType.name());
		state.put("earliestTime", this.earliestTime != null ? this.earliestTime.getStart() : JSONObject.NULL);
		state.put("earliestSpace", this.earliestSpace != null ? this.earliestSpace : JSONObject.NULL);
		state.put("latestTime", this.latestTime != null ? this.latestTime.getEnd() : JSONObject.NULL);
		state.put("latestSpace", this.latestSpace != null ? this.latestSpace : JSONObject.NULL);
		state.put("readOnly", this.isReadOnly());
		state.put("spatialUncertaintyPrecision", this.spatialUncertaintyPrecision);
		state.put("temporalUncertaintyPrecision", this.temporalUncertaintyPrecision);
		//state.put("temporalHint", this.temporalHint);
		state.put("temporalPosn", this.temporalPosn);
		state.put("spatialPosn1", this.spatialPosn1);
		state.put("spatialPosn2", this.spatialPosn2);
		
		logger.log(Level.INFO, "RIKI: BORDERMAP" + borderMap.keySet().size());
		logger.log(Level.INFO, "RIKI: BORDERMAP" + borderMap.size());
		
		if(borderMap.size() > 0) {
			JSONArray bMaps = new JSONArray();
			for(String path : borderMap.keySet()) {
				JSONObject bpJSON = borderMap.get(path).getJsonStringRepresentation(path);
				bMaps.put(bpJSON);
			}
			state.put("borderMaps", bMaps);
		}
		
		return state;
	}

	public static GeospatialFileSystem restoreState(StorageNode storageNode, NetworkInfo networkInfo, JSONObject state)
			throws FileSystemException, IOException, SerializationException, PartitionException, HashException,
			HashTopologyException {
		String name = state.getString("name");
		String storageRoot = state.getString("storageRoot");
		int geohashPrecision = state.getInt("precision");
		int nodesPerGroup = state.getInt("nodesPerGroup");
		JSONArray geohashIndices = state.getJSONArray("geohashIndex");
		String featureList = null;
		if (state.get("featureList") != JSONObject.NULL)
			featureList = state.getString("featureList");
		int temporalType = state.getInt("temporalType");
		SpatialHint spHint = null;
		if (state.get("spatialHint") != JSONObject.NULL) {
			JSONObject spHintJSON = state.getJSONObject("spatialHint");
			spHint = new SpatialHint(spHintJSON.getString("latHint"), spHintJSON.getString("lngHint"));
		}
		//logger.log(Level.INFO, "RIKI: NETWORK2: "+networkInfo);
		GeospatialFileSystem gfs = new GeospatialFileSystem(storageNode, storageRoot, name, geohashPrecision,
				nodesPerGroup, temporalType, networkInfo, featureList, spHint, true);
		gfs.earliestTime = (state.get("earliestTime") != JSONObject.NULL)
				? new TemporalProperties(state.getLong("earliestTime")) : null;
		gfs.earliestSpace = (state.get("earliestSpace") != JSONObject.NULL) ? state.getString("earliestSpace") : null;
		gfs.latestTime = (state.get("latestTime") != JSONObject.NULL)
				? new TemporalProperties(state.getLong("latestTime")) : null;
		gfs.latestSpace = (state.get("latestSpace") != JSONObject.NULL) ? state.getString("latestSpace") : null;
		Set<String> geohashIndex = new HashSet<String>();
		for (int i = 0; i < geohashIndices.length(); i++)
			geohashIndex.add(geohashIndices.getString(i));
		gfs.geohashIndex = geohashIndex;
		gfs.numCores = Runtime.getRuntime().availableProcessors();
		
		gfs.spatialUncertaintyPrecision = state.getInt("spatialUncertaintyPrecision");
		gfs.temporalUncertaintyPrecision = state.getInt("temporalUncertaintyPrecision");
		//gfs.temporalHint = state.getString("temporalHint");
		gfs.temporalPosn = state.getInt("temporalPosn");
		gfs.spatialPosn1 = state.getInt("spatialPosn1");
		gfs.spatialPosn2 = state.getInt("spatialPosn2");
		
		gfs.borderMap = new HashMap<String, BorderingProperties>();
		if(state.has("borderMaps")) {
			JSONArray bMaps = state.getJSONArray("borderMaps");
			if(bMaps != null && bMaps.length() > 0) {
				for (int i = 0; i < bMaps.length(); i++) {
					JSONObject jsonObject = bMaps.getJSONObject(i);
					String key = jsonObject.getString("blockName");
					BorderingProperties bp = new BorderingProperties(); 
					bp.populateObject(jsonObject);
					gfs.borderMap.put(key, bp);
				}
			}
		}
		
		return gfs;
	}

	public long getLatestTime() {
		if (this.latestTime != null)
			return this.latestTime.getEnd();
		return 0;
	}

	public long getEarliestTime() {
		if (this.earliestTime != null)
			return this.earliestTime.getStart();
		return 0;
	}

	public String getLatestSpace() {
		if (this.latestSpace != null)
			return this.latestSpace;
		return "";
	}

	public String getEarliestSpace() {
		if (this.earliestSpace != null)
			return this.earliestSpace;
		return "";
	}

	public int getGeohashPrecision() {
		return this.geohashPrecision;
	}
	
	private String getTemporalString(TemporalProperties tp) {
		if (tp == null)
			return "xxxx-xx-xx-xx";
		Calendar c = Calendar.getInstance();
		c.setTimeZone(TemporalHash.TIMEZONE);
		c.setTimeInMillis(tp.getStart());
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int day = c.get(Calendar.DAY_OF_MONTH);
		int month = c.get(Calendar.MONTH) + 1;
		int year = c.get(Calendar.YEAR);
		switch (this.temporalType) {
		case HOUR_OF_DAY:
			return String.format("%d-%d-%d-%d", year, month, day, hour);
		case DAY_OF_MONTH:
			return String.format("%d-%d-%d-xx", year, month, day);
		case MONTH:
			return String.format("%d-%d-xx-xx", year, month);
		case YEAR:
			return String.format("%d-xx-xx-xx", year);
		}
		return String.format("%d-%d-%d-xx", year, month, day);
	}

	private String getSpatialString(SpatialProperties sp) {
		char[] hash = new char[this.geohashPrecision];
		Arrays.fill(hash, 'o');
		String geohash = new String(hash);
		if (sp == null)
			return geohash;

		if (sp.hasRange()) {
			geohash = GeoHash.encode(sp.getSpatialRange(), this.geohashPrecision);
		} else {
			geohash = GeoHash.encode(sp.getCoordinates(), this.geohashPrecision);
		}
		return geohash;
	}

	/**
	 * @author sapmitra just checking the other method. remove this later
	 * @param sp
	 * @param geohashP
	 * @return
	 */
	public static String getSpatialString1(SpatialProperties sp, int geohashP) {
		char[] hash = new char[geohashP];
		Arrays.fill(hash, 'o');
		String geohash = new String(hash);
		if (sp == null)
			return geohash;

		if (sp.hasRange()) {
			geohash = GeoHash.encode(sp.getSpatialRange(), geohashP);
		} else {
			geohash = GeoHash.encode(sp.getCoordinates(), geohashP);
		}
		return geohash;
	}
	
	public static void main(String arg[]) throws FileSystemException, IOException, SerializationException, PartitionException, HashException, HashTopologyException, ParseException {
		Coordinates c1 = new Coordinates(39.711308f, -94.14132f);
		Coordinates c2 = new Coordinates(39.135788f, -94.06672f);
		Coordinates c3 = new Coordinates(39.101475f, -95.56087f);
		Coordinates c4 = new Coordinates(39.861824f, -95.36784f);
		
		List<Coordinates> cl = new ArrayList<Coordinates>();
		cl.add(c1);
		cl.add(c2);
		cl.add(c3);
		cl.add(c4);
		
		GeoavailabilityQuery geoQuery = new GeoavailabilityQuery(null,cl);
		GeoavailabilityGrid blockGrid = new GeoavailabilityGrid("9zh8", GeoHash.MAX_PRECISION * 2 / 3);
		//queryBitmap = QueryTransform.queryToGridBitmap(geoQuery, blockGrid);

		//boolean b = isGridInsidePolygonTest(blockGrid, geoQuery);
		//System.out.println(b);

	}
	
	
	
	
	/**
	 * Creates a new block if one does not exist based on the name of the
	 * metadata or appends the bytes to an existing block in which case the
	 * metadata in the graph will not be updated.
	 */
	@Override
	public String storeBlock(Block block) throws FileSystemException, IOException {
		Metadata meta = block.getMetadata();
		/* RETURNS A STRING OF THE FORMAT year-month-day-hour */
		String time = getTemporalString(meta.getTemporalProperties());
		String geohash = getSpatialString(meta.getSpatialProperties());
		
		/* Name of the block to be saved */
		String name = String.format("%s-%s", time, geohash);
		if (meta.getName() != null && meta.getName().trim() != "")
			name = meta.getName();
		String blockDirPath = this.storageDirectory + File.separator + getStorageDirectory(block);
		String blockPath = blockDirPath + File.separator + name + FileSystem.BLOCK_EXTENSION;
		String metadataPath = blockDirPath + File.separator + name + FileSystem.METADATA_EXTENSION;

		/* Ensure the storage directory is there. */
		File blockDirectory = new File(blockDirPath);
		if (!blockDirectory.exists()) {
			if (!blockDirectory.mkdirs()) {
				throw new IOException("Failed to create directory (" + blockDirPath + ") for block.");
			}
			
		}

		// Adding temporal and spatial features at the top
		// to the existing attributes
		
		FeatureSet newfs = new FeatureSet();
		String[] temporalFeature = time.split("-");
		newfs.put(new Feature(TEMPORAL_YEAR_FEATURE, temporalFeature[0]));
		newfs.put(new Feature(TEMPORAL_MONTH_FEATURE, temporalFeature[1]));
		newfs.put(new Feature(TEMPORAL_DAY_FEATURE, temporalFeature[2]));
		newfs.put(new Feature(TEMPORAL_HOUR_FEATURE, temporalFeature[3]));
		newfs.put(new Feature(SPATIAL_FEATURE, geohash));

		for (Feature feature : meta.getAttributes())
			newfs.put(feature);
		meta.setAttributes(newfs);

		Serializer.persist(block.getMetadata(), metadataPath);
		File gblock = new File(blockPath);
		boolean newLine = gblock.exists();
		
		/*for(String path: borderMap.keySet()) {
			logger.log(Level.INFO, "RIKI: PATH: "+path+ "$$"+ borderMap.get(path));
		}*/
		
		/* ADDING METADATA TO METADATA GRAPH */
		if (!newLine) {
			/* Creating a bordering property for this block with alist of all neighboring
			 * geohashes and times */
			BorderingProperties bp = GeoHash.getBorderingGeohashHeuristic(geohash, spatialUncertaintyPrecision, temporalUncertaintyPrecision , meta.getTemporalProperties(), this.temporalType);
			borderMap.put(blockPath, bp);
			logger.log(Level.INFO, "RIKI: BORDERMAP CREATED "+bp);
			
			storeMetadata(meta, blockPath);
		}
		/*
		 * TODO: Add an attribute to this class asking for block update strategy
		 * - whether to overwrite blocks, or append content. When it is append,
		 * ask for any delimiter to separate the existing data.
		 **/
		try (FileOutputStream blockData = new FileOutputStream(blockPath, true)) {
			if (newLine)
				blockData.write("\n".getBytes("UTF-8"));
			blockData.write(block.getData());
		} catch (Exception e) {
			throw new FileSystemException("Error storing block: " + e.getClass().getCanonicalName(), e);
		}
		
		/* RIKI */
		readBlockData(block.getData(), borderMap.get(blockPath));

		if (latestTime == null || latestTime.getEnd() < meta.getTemporalProperties().getEnd()) {
			this.latestTime = meta.getTemporalProperties();
			this.latestSpace = geohash;
		}

		if (earliestTime == null || earliestTime.getStart() > meta.getTemporalProperties().getStart()) {
			this.earliestTime = meta.getTemporalProperties();
			this.earliestSpace = geohash;
		}

		this.geohashIndex.add(geohash.substring(0, Partitioner.SPATIAL_PRECISION));

		return blockPath;
	}
	
	public static long reformatDatetime(String date){
		String tmp = date.replace(".", "").replace("E9", "");
		while(tmp.length()<13){
			tmp+="0";
		}
		return Long.parseLong(tmp); 
	}

	
	private static float parseFloat(String input){
		try {
			return Float.parseFloat(input);
		} catch(Exception e){
			return 0.0f;
		}
	}
	
	/* Reading block data in order to populate border indices */
	/**
	 * 
	 * @author sapmitra
	 * @param data
	 * @param borderingProperties
	 */
	
	private void readBlockData(byte[] data, BorderingProperties borderingProperties) {
		// TODO Auto-generated method stub
		String blockString = new String(data);
		String[] records = blockString.split("\n");
		long currentRecordsCount = records.length;
		
		long recordCount = borderingProperties.getTotalRecords();
		for(String record: records) {
			String[] fields = record.split(",");
			long timestamp = reformatDatetime(fields[temporalPosn]);
			
			String geoHash = GeoHash.encode(parseFloat(fields[spatialPosn1]),parseFloat(fields[spatialPosn2]), spatialUncertaintyPrecision);
			
			// Getting spatial border records
			populateGeoHashBorder(geoHash, borderingProperties, recordCount);
			
			// calculating temporal border records
			if(timestamp<=borderingProperties.getDown2() && timestamp >= borderingProperties.getDown1()) {
				borderingProperties.addDownTimeEntries(recordCount);
				logger.info("RIKI: ENTERED DOWN TIME ENTRY");
			} else if(timestamp<=borderingProperties.getUp1() && timestamp >= borderingProperties.getUp2()) {
				borderingProperties.addUpTimeEntries(recordCount);
				logger.info("RIKI: ENTERED UP TIME ENTRY");
			}
			recordCount++;
		}
		
		borderingProperties.updateRecordCount(currentRecordsCount);
		
	}

	/* Populating border Indices */
	/* North and nw are disjoint */
	
	private void populateGeoHashBorder(String geoHash, BorderingProperties borderingProperties, long recordCount) {
		
		if(borderingProperties.getNe().equals(geoHash)) {
			borderingProperties.addNEEntries(recordCount);
			logger.info("RIKI: ENTERED A NE ENTRY "+geoHash +" "+this.name);
		} else if(borderingProperties.getSe().equals(geoHash)) {
			borderingProperties.addSEEntries(recordCount);
			logger.info("RIKI: ENTERED A SE ENTRY "+geoHash+" "+this.name);
		} else if(borderingProperties.getNw().equals(geoHash)) {
			borderingProperties.addNWEntries(recordCount);
			logger.info("RIKI: ENTERED A NW ENTRY "+geoHash+" "+this.name);
		} else if(borderingProperties.getSw().equals(geoHash)) {
			borderingProperties.addSWEntries(recordCount);
			logger.info("RIKI: ENTERED A SW ENTRY "+geoHash+" "+this.name);
		} else if(borderingProperties.getN().contains(geoHash)) {
			borderingProperties.addNorthEntries(recordCount);
			logger.info("RIKI: ENTERED A N ENTRY "+geoHash+" "+this.name);
		} else if(borderingProperties.getE().contains(geoHash)) {
			borderingProperties.addEastEntries(recordCount);
			logger.info("RIKI: ENTERED A E ENTRY "+geoHash+" "+this.name);
		} else if(borderingProperties.getW().contains(geoHash)) {
			borderingProperties.addWestEntries(recordCount);
			logger.info("RIKI: ENTERED A W ENTRY "+geoHash+" "+this.name);
		} else if(borderingProperties.getS().contains(geoHash)) {
			borderingProperties.addSouthEntries(recordCount);
			logger.info("RIKI: ENTERED A S ENTRY "+geoHash+" "+this.name);
		} else {
			logger.info("RIKI: ENTERED A CENTRAL ENTRY "+geoHash+" "+this.name);
		}
		
		
	}

	public Block retrieveBlock(String blockPath) throws IOException, SerializationException {
		Metadata metadata = null;
		byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
		String metadataPath = blockPath.replace(BLOCK_EXTENSION, METADATA_EXTENSION);
		File metadataFile = new File(metadataPath);
		if (metadataFile.exists())
			metadata = Serializer.deserialize(Metadata.class, Files.readAllBytes(Paths.get(metadataPath)));
		return new Block(this.name, metadata, blockBytes);
	}

	/**
	 * Given a {@link Block}, determine its storage directory on disk.
	 *
	 * @param block
	 *            The Block to inspect
	 *
	 * @return String representation of the directory on disk this Block should
	 *         be stored in.
	 */
	private String getStorageDirectory(Block block) {
		String directory = "";

		Metadata meta = block.getMetadata();
		directory = getTemporalDirectoryStructure(meta.getTemporalProperties()) + File.separator;

		Coordinates coords = null;
		SpatialProperties spatialProps = meta.getSpatialProperties();
		if (spatialProps.hasRange()) {
			coords = spatialProps.getSpatialRange().getCenterPoint();
		} else {
			coords = spatialProps.getCoordinates();
		}
		directory += GeoHash.encode(coords, geohashPrecision);

		return directory;
	}

	private String getTemporalDirectoryStructure(TemporalProperties tp) {
		return timeFormatter.format(tp.getLowerBound());
	}

	private List<Expression> buildTemporalExpression(String temporalProperties) {
		
		/* Riki */
		// Making sure that this temporalexpression adheres to the temporaltype of a particular filesystem
		// Meaning if the temporaltype is year, the temporalexpression must only consist of the temporalyearfeature
		TemporalType ttype = this.temporalType;
		int tt = 0;
		
		if(ttype == TemporalType.YEAR)
			tt = 0;
		else if(ttype == TemporalType.MONTH)
			tt = 1;
		else if(ttype == TemporalType.DAY_OF_MONTH)
			tt = 2;
		else if(ttype == TemporalType.HOUR_OF_DAY)
			tt = 3;
		
		
		List<Expression> temporalExpressions = new ArrayList<Expression>();
		String[] temporalFeatures = temporalProperties.split("-");
		int length = (temporalFeatures.length <= 4) ? temporalFeatures.length : 4;
		for (int i = 0; i < length; i++) {
			if (temporalFeatures[i].charAt(0) != 'x' && i <= tt) {
				String temporalFeature = temporalFeatures[i];
				if (temporalFeature.charAt(0) == '0')
					temporalFeature = temporalFeature.substring(1);
				Feature feature = null;
				switch (i) {
				case 0:
					feature = new Feature(TEMPORAL_YEAR_FEATURE, temporalFeature);
					break;
				case 1:
					feature = new Feature(TEMPORAL_MONTH_FEATURE, temporalFeature);
					break;
				case 2:
					feature = new Feature(TEMPORAL_DAY_FEATURE, temporalFeature);
					break;
				case 3:
					feature = new Feature(TEMPORAL_HOUR_FEATURE, temporalFeature);
					break;
				}
				temporalExpressions.add(new Expression(Operator.EQUAL, feature));
			}
		}
		return temporalExpressions;
	}
	
	
	private TemporalType getTemporalType(String[] tokens) {
		
		if (tokens.length == 4) {
			
			if (tokens[1].contains("x")) {

				return TemporalType.YEAR;

			} else if (tokens[2].contains("x")) {

				return TemporalType.MONTH;

			} else if (tokens[3].contains("x")) {

				return TemporalType.DAY_OF_MONTH;

			} else {

				return TemporalType.HOUR_OF_DAY;

			}
		}
		return null;
	}
	
	private int getTemporalAdditionType() {
		
		if (temporalType == TemporalType.YEAR) {
			return Calendar.YEAR;
		} else if (temporalType == TemporalType.MONTH) {
			return Calendar.MONTH;
		} else if (temporalType == TemporalType.DAY_OF_MONTH) {
			return Calendar.DATE;
		}  else if (temporalType == TemporalType.HOUR_OF_DAY) {
			return Calendar.HOUR;
		}  
		
		return -1;
	}
	
	/**
	 * based on the time sent for query, and the temporalType of fs2, this builds 3 temporal expressions
	 * that represent the before, now and after
	 * 
	 * queryTime is a string like 2014-01-xx-xx or 2014-01-01-4
	 * @author sapmitra
	 * @param queryTime
	 * @return
	 * @throws ParseException 
	 */
	private List<List<Expression>> buildTemporalExpressionForFS2(String queryTime) throws ParseException {
		
		// These return the dates that lie between the start and end of the timestamps
		// Now since every date except the first and the last have to cover the entire day,
		// for them the temporal expression must just cover till day of month
		// For the first and last date, we have to go down to hours if necessary
		
		String[] tokens = queryTime.split("-");
		
		// Getting temporalType based on the query string.
		// This is not the TemporalType of this filesystem
		TemporalType ttQuery = getTemporalType(tokens);
		
		/* This represents the bounds for the actual query entered */
		long startBasedOnQuery = GeoHash.getStartTimeStamp(tokens[2], tokens[1], tokens[0], tokens[3], ttQuery) - temporalUncertaintyPrecision;
		long endBasedOnQuery = GeoHash.getEndTimeStamp(tokens[2], tokens[1], tokens[0], tokens[3], ttQuery) + temporalUncertaintyPrecision;
		
		List<List<Expression>> allTemporalExpressions = new ArrayList<List<Expression>>();
		
		List<Expression> baseExpressions = buildTemporalExpression(queryTime);
		allTemporalExpressions.add(baseExpressions);
		
		int tt = 0;
		
		if(temporalType == TemporalType.YEAR)
			tt = 0;
		else if(temporalType == TemporalType.MONTH)
			tt = 1;
		else if(temporalType == TemporalType.DAY_OF_MONTH)
			tt = 2;
		else if(temporalType == TemporalType.HOUR_OF_DAY)
			tt = 3;
		
		String[] tokensFS = {"xxxx","xx","xx","xx"};
		for(int i=0; i <= tt; i++) {
			
			tokensFS[i] = tokens[i];
			
		}
		
		/* This represents the span of time already searched by the baseExpression */
		long startBasedOnBExp = GeoHash.getStartTimeStamp(tokensFS[2], tokensFS[1], tokensFS[0], tokensFS[3], temporalType);
		long endBasedOnBExp = GeoHash.getEndTimeStamp(tokensFS[2], tokensFS[1], tokensFS[0], tokensFS[3], temporalType);
		
		
		// check if the temporal expression for the base query string encloses the bounding flaps of the querytime
		while(startBasedOnQuery < startBasedOnBExp) {
			
			Calendar cal = Calendar.getInstance();
		    cal.setTimeInMillis(startBasedOnQuery);
		    
		    String queryString = cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH)+"-"+cal.get(Calendar.HOUR_OF_DAY);
		    List<Expression> extraExpressions = buildTemporalExpression(queryString);
		    
		    
		    allTemporalExpressions.add(extraExpressions);
		    cal.add(getTemporalAdditionType(), 1);
		    
		    startBasedOnQuery = cal.getTimeInMillis();
		}
		
		while(endBasedOnQuery > endBasedOnBExp) {
			
			Calendar cal = Calendar.getInstance();
		    cal.setTimeInMillis(endBasedOnQuery);
		    
		    String queryString = cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH)+"-"+cal.get(Calendar.HOUR_OF_DAY);
		    List<Expression> extraExpressions = buildTemporalExpression(queryString);
		    
		    
		    allTemporalExpressions.add(extraExpressions);
		    cal.add(getTemporalAdditionType(), -1);
		    
		    endBasedOnQuery = cal.getTimeInMillis();
			
		}
		
		return allTemporalExpressions;
	}
	
	

	private String getGroupKey(Path<Feature, String> path, String space) {
		if (null != path && path.hasPayload()) {
			List<Feature> labels = path.getLabels();
			String year = "xxxx", month = "xx", day = "xx", hour = "xx";
			int allset = (space == null) ? 0 : 1;
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
					if (space == null) {
						space = label.getString();
						allset++;
					}
					break;
				}
				if (allset == 5)
					break;
			}
			return String.format("%s-%s-%s-%s-%s", year, month, day, hour, space);
		}
		return String.format("%s-%s", getTemporalString(null), (space == null) ? getSpatialString(null) : space);
	}

	private String getSpaceKey(Path<Feature, String> path) {
		if (null != path && path.hasPayload()) {
			List<Feature> labels = path.getLabels();
			for (Feature label : labels)
				if (label.getName().toLowerCase().equals(SPATIAL_FEATURE))
					return label.getString();
		}
		return getSpatialString(null);
	}

	private Query queryIntersection(Query q1, Query q2) {
		if (q1 != null && q2 != null) {
			Query query = new Query();
			for (Operation q1Op : q1.getOperations()) {
				for (Operation q2Op : q2.getOperations()) {
					Operation op = new Operation(q1Op.getExpressions());
					op.addExpressions(q2Op.getExpressions());
					query.addOperation(op);
				}
			}
			logger.info(query.toString());
			return query;
		} else if (q1 != null) {
			return q1;
		} else if (q2 != null) {
			return q2;
		} else {
			return null;
		}
	}

	private class ParallelQueryEvaluator implements Runnable {
		private List<Operation> operations;
		private List<Path<Feature, String>> resultPaths;

		public ParallelQueryEvaluator(List<Operation> operations) {
			this.operations = operations;
		}

		public List<Path<Feature, String>> getResults() {
			return this.resultPaths;
		}

		@Override
		public void run() {
			Query query = new Query();
			query.addAllOperations(operations);
			this.resultPaths = metadataGraph.evaluateQuery(query);
		}
	}

	private List<Path<Feature, String>> executeParallelQuery(Query finalQuery) throws InterruptedException {
		logger.info("Query: " + finalQuery.toString());
		List<Path<Feature, String>> paths = new ArrayList<>();
		List<Operation> operations = finalQuery.getOperations();
		if (operations.size() > 0) {
			if (operations.size() > numCores) {
				int subsetSize = operations.size() / numCores;
				for (int i = 0; i < numCores; i++) {
					operations.subList(i, (i + 1) * subsetSize);
				}
				int size = operations.size();
				ExecutorService executor = Executors.newFixedThreadPool(numCores);
				List<ParallelQueryEvaluator> queryEvaluators = new ArrayList<>();
				for (int i = 0; i < numCores; i++) {
					int from = i * subsetSize;
					int to = (i + 1 != numCores) ? (i + 1) * subsetSize : size;
					List<Operation> subset = new ArrayList<>(operations.subList(from, to));
					ParallelQueryEvaluator pqe = new ParallelQueryEvaluator(subset);
					queryEvaluators.add(pqe);
					executor.execute(pqe);
				}
				operations.clear();
				executor.shutdown();
				boolean termination = executor.awaitTermination(10, TimeUnit.MINUTES);
				if (!termination)
					logger.severe("Query failed to process in 10 minutes");
				paths = new ArrayList<>();
				for (ParallelQueryEvaluator pqe : queryEvaluators)
					if (pqe.getResults() != null)
						paths.addAll(pqe.getResults());
			} else {
				paths = metadataGraph.evaluateQuery(finalQuery);
			}
		}
		return paths;
	}

	/**
	 * 
	 * @param temporalProperties
	 * @param spatialProperties
	 * @param metaQuery
	 * @param group: whether it is a dry run or not
	 * @return
	 * @throws InterruptedException
	 */
	
	public Map<String, List<String>> listBlocks(String temporalProperties, List<Coordinates> spatialProperties,
			Query metaQuery, boolean group) throws InterruptedException {
		 Map<String, List<String>> blockMap = new HashMap<String, List<String>>();
		String space = null;
		List<Path<Feature, String>> paths = null;
		List<String> blocks = new ArrayList<String>();
		/* temporal and spatial properties from the query event */
		if (temporalProperties != null && spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			/* Tries to get the geohash for the center-point of the MBR for the polygon */
			space = getSpatialString(sp);
			
			/* Returns all 2 char geohashes that intersect with the searched polygon */
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			
			/* Builds an expression for the temporal query asking the top level temporal levels to be 
			 * equal to whatever is in the time string */
			// temporalProperties is a string of the format YEAR-MONTH-DAY-HOUR
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				
				// gets a 111000000 like binary representation of the geohash
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation(temporalExpressions);
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			/* query intersection merges the query with metadata query */
			/* returns a list of paths matching the query */
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else if (temporalProperties != null) {
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			Query query = new Query(
					new Operation(temporalExpressions.toArray(new Expression[temporalExpressions.size()])));
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else if (spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			space = getSpatialString(sp);
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation();
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			/* Queryintersection combines the normal and metadata query */
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else {
			// non-chronal non-spatial
			paths = (metaQuery == null) ? metadataGraph.getAllPaths() : executeParallelQuery(metaQuery);
		}
		
		// Paths look like Path((root,f1,f2,f3,...),payload). Each path represents each DFS traversal of a tree
		
		for (Path<Feature, String> path : paths) {
			String groupKey = group ? getGroupKey(path, space) : getSpaceKey(path);
			blocks = blockMap.get(groupKey);
			if (blocks == null) {
				blocks = new ArrayList<String>();
				blockMap.put(groupKey, blocks);
			}
			blocks.addAll(path.getPayload());
		}
		return blockMap;
	}
	

	/**
	 * This method just returns the Paths with Orientations
	 * No actual record is searched here
	 * 
	 * @author sapmitra
	 * @param superCubes
	 * @param superPolygon
	 * @param queryTime is a string like 2017-11-140-xx
	 * @return
	 * @throws InterruptedException
	 * @throws ParseException 
	 */
	public PathsAndOrientations listIntersectingPathsWithOrientation(List<SuperCube> superCubes, List<Coordinates> superPolygon, String queryTime, Query metaQuery, TemporalType srcTT) throws InterruptedException, ParseException {
		
		List<List<Expression>> temporalExpressionList = null;
		List<Path<Feature, String>> paths = null;
		
		try {
			temporalExpressionList =  buildTemporalExpressionForFS2(queryTime);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			logger.log(Level.SEVERE, "TEMPORAL EXPRESSION CREATION FAILED:\n" + e.getMessage());
		}
		
		/* Both space and time in query */
		
		if (superPolygon != null && superPolygon.size() > 0 && temporalExpressionList != null && temporalExpressionList.size() > 0) {
			logger.log(Level.SEVERE, "RIKI: ENTERED HERE");
			SpatialProperties sp = new SpatialProperties(new SpatialRange(superPolygon));
			
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon() : sp.getSpatialRange().getBounds();
			
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			
			Query query = new Query();
			
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			
			for(List<Expression> temporalExpressions : temporalExpressionList) {
				for (String geohash : hashLocations) {
					Set<GeoHash> intersections = new HashSet<>();
					String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
					
					// gets a 111000000 like binary representation of the geohash
					String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
					GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
							this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
					logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
					for (GeoHash gh : intersections) {
						String[] hashRange = gh.getValues(this.geohashPrecision);
						if (hashRange != null) {
							Operation op = new Operation(temporalExpressions);
							if (hashRange.length == 1)
								op.addExpressions(
										new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							else {
								op.addExpressions(
										new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
								op.addExpressions(
										new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
							}
							query.addOperation(op);
						}
					}
				}
			}
			/* query intersection merges the query with metadata query */
			/* returns a list of paths matching the query */
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else if (temporalExpressionList != null && temporalExpressionList.size() > 0) {
			Query query = new Query();
			for(List<Expression> temporalExpressions : temporalExpressionList) {
				Operation op = new Operation(temporalExpressions);
				query.addOperation(op);
				
			}
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else if(superPolygon != null && superPolygon.size() > 0) {
			
			SpatialProperties sp = new SpatialProperties(new SpatialRange(superPolygon));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation();
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			/* Queryintersection combines the normal and metadata query */
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
			
		} else {
			
			paths = (metaQuery == null) ? metadataGraph.getAllPaths() : executeParallelQuery(metaQuery);
			
		}
		
		
		/*
		 * Now that we know which paths are needed,
		 * lets find out what fragments from these paths are needed for each of the supercube that requested it
		 */
		
		Map<Path<Feature, String>, PathFragments> pathToFragmentsMap = new HashMap<Path<Feature, String>, PathFragments>();
		Map<SuperCube,List<Requirements>> supercubeRequirementsMap = new HashMap<SuperCube,List<Requirements>>();
		//logger.log(Level.SEVERE, "RIKI: ABOUT TO ENTER LOOP" + superCubes);
		
		// For each supercube
		// CALCULATE THE FRAGMENTS THAT ARE NEEDED
		for (SuperCube sc : superCubes) {
			// For each path that matched the query hyper-polygon and time limit
			for (Path<Feature, String> path : paths) {
				//logger.log(Level.SEVERE, "RIKI: INSODE LOOP");
				//logger.info("RIKI: FS2 PATHS FOR FRAGMENTS: "+path.getPayload());
				
				String fs1PathTime = sc.getCentralTime();
				String fs1PathSpace = sc.getCentralGeohash();
				
				/* RETURND TEMPORAL$SPATIAL  STRING*/
				String ret = getPathInfo(path, 0 );
				//logger.log(Level.INFO,"RIKI: FS1 PATHINFO: "+fs1PathSpace + fs1PathTime);
				//logger.log(Level.INFO, "RIKI: FS2 PATHINFO: "+ret);
				if(ret != null){
					String[] tokens = ret.split("\\$");
					String fs2PathTime = tokens[0];
					String fs2PathSpace = tokens[1];
					
					String orientation = GeoHash.getOrientation(fs1PathSpace, fs2PathSpace, fs1PathTime, fs2PathTime, srcTT, this.temporalType);
					logger.log(Level.INFO, "RIKI: ORIENTATION: "+orientation + path.getPayload());
					if(orientation.contains("ignore"))
						continue;
					
					// These are the fragments of path required by the particular supercube sc
					List<Integer> fragments = OrientationManager.getRequiredChunks(orientation);
					logger.log(Level.INFO, "RIKI: FRAGMENTS FROM ORIENTATION: "+fragments + path.getPayload());
					if(fragments == null || fragments.size() == 0) 
						continue;
					
					// populate path to fragments map
					
						
					if(pathToFragmentsMap.get(path) == null) {
						
						PathFragments pf = new PathFragments();
						//logger.log(Level.INFO, "RIKI: FRAGMENTS FOUND1: "+fragments);
						// entire blocks to be read, no fragments
						if("full-full".equals(orientation)) {
							// IGNORE MEANS ALL FRAGMENTS ARE REQUIRED TO BE RETURNED
							pf.setIgnore(true);
							pf.addChunks(fragments);
						} else {
							pf.setIgnore(false);
							pf.addChunks(fragments);
						}
						
						pathToFragmentsMap.put(path, pf);
						//logger.info("RIKI: INDIVIDUAL: "+pf);
						/*for(Path<Feature, String> p: pathToFragmentsMap.keySet()) {
							logger.log(Level.INFO, "RIKI : PATHZ: " +pathToFragmentsMap.get(p).getChunks()+" "+pathToFragmentsMap.get(p).getOrientations());
						}*/
					} else {
						PathFragments pf = pathToFragmentsMap.get(path);
						//logger.log(Level.INFO, "RIKI: FRAGMENTS FOUND2: "+fragments);
						pf.addChunks(fragments);
						pf.setIgnore(false);
						pathToFragmentsMap.put(path, pf);
					}
					// handle supercube requirements map
					
					List<Requirements> requirements;
					if(supercubeRequirementsMap.get(sc) == null) {
						requirements = new ArrayList<Requirements>();
					} else {
						requirements = supercubeRequirementsMap.get(sc);
					}
					requirements.add(new Requirements(paths.indexOf(path), fragments));
					supercubeRequirementsMap.put(sc, requirements);
					
					
				} else {
					supercubeRequirementsMap.put(sc, null);
					continue;
				}

			}
		}
		for(Path<Feature, String> p: pathToFragmentsMap.keySet()) {
			logger.log(Level.INFO, "RIKI : PATHHZZ: " +pathToFragmentsMap.get(p).getChunks()+" "+pathToFragmentsMap.get(p).getOrientations());
		}
		
		int totalBlocks = 0;
		
		for (Path<Feature, String> path : paths) {
			totalBlocks+=path.getPayload().size();
		}
		
		if(totalBlocks > 0) {
			
			PathsAndOrientations pao = new PathsAndOrientations(paths, pathToFragmentsMap, supercubeRequirementsMap, totalBlocks);
			return pao;
			
		}
		
		return null;
	}
	
	
	public static String getPathInfo(Path<Feature, String> path, int o) {
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
			String temporal = day+"-"+month+"-"+year+"-"+hour;
			
			if(o == 1)
				return space;
			
			return temporal+"$"+space;
			
			
			
		}
		
		return null;
	}
	
	
	
	
	/**
	 * 
	 * @author sapmitra
	 * @param temporalProperties
	 * @param spatialProperties
	 * @param metaQuery
	 * @param group
	 * @return
	 * @throws InterruptedException
	 */
	
	public List<Path<Feature, String>> listPaths(String temporalProperties, List<Coordinates> spatialProperties,
			Query metaQuery, boolean group) throws InterruptedException {
		List<Path<Feature, String>> paths = null;
		/* temporal and spatial properties from the query event */
		if (temporalProperties != null && spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			/* Tries to get the geohash for the center-point of the MBR for the polygon */
			
			/* Returns all 2 char geohashes that intersect with the searched polygon */
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			
			/* Builds an expression for the temporal query asking the top level temporal levels to be 
			 * equal to whatever is in the time string */
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				
				// gets a 111000000 like binary representation of the geohash
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation(temporalExpressions);
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			/* query intersection merges the query with metadata query */
			/* returns a list of paths matching the query */
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else if (temporalProperties != null) {
			List<Expression> temporalExpressions = buildTemporalExpression(temporalProperties);
			Query query = new Query(
					new Operation(temporalExpressions.toArray(new Expression[temporalExpressions.size()])));
			paths = metadataGraph.evaluateQuery(queryIntersection(query, metaQuery));
		} else if (spatialProperties != null) {
			SpatialProperties sp = new SpatialProperties(new SpatialRange(spatialProperties));
			List<Coordinates> geometry = sp.getSpatialRange().hasPolygon() ? sp.getSpatialRange().getPolygon()
					: sp.getSpatialRange().getBounds();
			List<String> hashLocations = new ArrayList<>(Arrays.asList(GeoHash.getIntersectingGeohashes(geometry)));
			hashLocations.retainAll(this.geohashIndex);
			logger.info("baseLocations: " + hashLocations);
			Query query = new Query();
			Polygon polygon = GeoHash.buildAwtPolygon(geometry);
			for (String geohash : hashLocations) {
				Set<GeoHash> intersections = new HashSet<>();
				String pattern = "%" + (geohash.length() * GeoHash.BITS_PER_CHAR) + "s";
				String binaryHash = String.format(pattern, Long.toBinaryString(GeoHash.hashToLong(geohash)));
				GeoHash.getGeohashPrefixes(polygon, new GeoHash(binaryHash.replace(" ", "0")),
						this.geohashPrecision * GeoHash.BITS_PER_CHAR, intersections);
				logger.info("baseHash: " + geohash + ", intersections: " + intersections.size());
				for (GeoHash gh : intersections) {
					String[] hashRange = gh.getValues(this.geohashPrecision);
					if (hashRange != null) {
						Operation op = new Operation();
						if (hashRange.length == 1)
							op.addExpressions(
									new Expression(Operator.EQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
						else {
							op.addExpressions(
									new Expression(Operator.GREATEREQUAL, new Feature(SPATIAL_FEATURE, hashRange[0])));
							op.addExpressions(
									new Expression(Operator.LESSEQUAL, new Feature(SPATIAL_FEATURE, hashRange[1])));
						}
						query.addOperation(op);
					}
				}
			}
			/* Queryintersection combines the normal and metadata query */
			paths = executeParallelQuery(queryIntersection(query, metaQuery));
		} else {
			// non-chronal non-spatial
			paths = (metaQuery == null) ? metadataGraph.getAllPaths() : executeParallelQuery(metaQuery);
		}
		
		// Paths look like Path((root,f1,f2,f3,...),payload). Each path represents each DFS traversal of a tree
		
		
		return paths;
	}

	private class Tracker {
		private int occurrence;
		private long fileSize;
		private long timestamp;

		public Tracker(long filesize, long millis) {
			this.occurrence = 1;
			this.fileSize = filesize;
			this.timestamp = millis;
		}

		public void incrementOccurrence() {
			this.occurrence++;
		}

		public int getOccurrence() {
			return this.occurrence;
		}

		public void incrementFilesize(long value) {
			this.fileSize += value;
		}

		public long getFilesize() {
			return this.fileSize;
		}

		public void updateTimestamp(long millis) {
			if (this.timestamp < millis)
				this.timestamp = millis;
		}

		public long getTimestamp() {
			return this.timestamp;
		}
	}

	public JSONArray getOverview() {
		JSONArray overviewJSON = new JSONArray();
		Map<String, Tracker> geohashMap = new HashMap<String, Tracker>();
		Calendar timestamp = Calendar.getInstance();
		timestamp.setTimeZone(TemporalHash.TIMEZONE);
		List<Path<Feature, String>> allPaths = metadataGraph.getAllPaths();
		logger.info("all paths size: " + allPaths.size());
		try {
			for (Path<Feature, String> path : allPaths) {
				long payloadSize = 0;
				if (path.hasPayload()) {
					for (String payload : path.getPayload()) {
						try {
							payloadSize += Files.size(java.nio.file.Paths.get(payload));
						} catch (IOException e) { /* e.printStackTrace(); */
							System.err.println("Exception occurred reading the block size. " + e.getMessage());
						}
					}
				}
				String geohash = path.get(4).getLabel().getString();
				String yearFeature = path.get(0).getLabel().getString();
				String monthFeature = path.get(1).getLabel().getString();
				String dayFeature = path.get(2).getLabel().getString();
				String hourFeature = path.get(3).getLabel().getString();
				if (yearFeature.charAt(0) == 'x') {
					System.err.println("Cannot build timestamp without year. Ignoring path");
					continue;
				}
				if (monthFeature.charAt(0) == 'x')
					monthFeature = "12";
				if (hourFeature.charAt(0) == 'x')
					hourFeature = "23";
				int year = Integer.parseInt(yearFeature);
				int month = Integer.parseInt(monthFeature) - 1;
				if (dayFeature.charAt(0) == 'x') {
					Calendar cal = Calendar.getInstance();
					cal.setTimeZone(TemporalHash.TIMEZONE);
					cal.set(Calendar.YEAR, year);
					cal.set(Calendar.MONTH, month);
					dayFeature = String.valueOf(cal.getActualMaximum(Calendar.DAY_OF_MONTH));
				}
				int day = Integer.parseInt(dayFeature);
				int hour = Integer.parseInt(hourFeature);
				timestamp.set(year, month, day, hour, 59, 59);

				Tracker geohashTracker = geohashMap.get(geohash);
				if (geohashTracker == null) {
					geohashMap.put(geohash, new Tracker(payloadSize, timestamp.getTimeInMillis()));
				} else {
					geohashTracker.incrementOccurrence();
					geohashTracker.incrementFilesize(payloadSize);
					geohashTracker.updateTimestamp(timestamp.getTimeInMillis());
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "failed to process a path", e);
		}

		logger.info("geohash map size: " + geohashMap.size());
		for (String geohash : geohashMap.keySet()) {
			Tracker geohashTracker = geohashMap.get(geohash);
			JSONObject geohashJSON = new JSONObject();
			geohashJSON.put("region", geohash);
			List<Coordinates> boundingBox = GeoHash.decodeHash(geohash).getBounds();
			JSONArray bbJSON = new JSONArray();
			for (Coordinates coordinates : boundingBox) {
				JSONObject vertex = new JSONObject();
				vertex.put("lat", coordinates.getLatitude());
				vertex.put("lng", coordinates.getLongitude());
				bbJSON.put(vertex);
			}
			geohashJSON.put("spatialCoordinates", bbJSON);
			geohashJSON.put("blockCount", geohashTracker.getOccurrence());
			geohashJSON.put("fileSize", geohashTracker.getFilesize());
			geohashJSON.put("latestTimestamp", geohashTracker.getTimestamp());
			overviewJSON.put(geohashJSON);
		}
		return overviewJSON;
	}

	/**
	 * Using the Feature attributes found in the provided Metadata, a path is
	 * created for insertion into the Metadata Graph.
	 */
	protected FeaturePath<String> createPath(String physicalPath, Metadata meta) {
		FeaturePath<String> path = new FeaturePath<String>(physicalPath, meta.getAttributes().toArray());
		return path;
	}

	@Override
	public void storeMetadata(Metadata metadata, String blockPath) throws FileSystemException, IOException {
		/* FeaturePath has a list of Vertices, each label in a vertex representing a feature */
		/* BlockPath is the actual path to the block */
		
		/* path represents a single leg of metadata for a block to be inserted */
		FeaturePath<String> path = createPath(blockPath, metadata);
		
		/* Saving the path to disk */
		pathJournal.persistPath(path);
		
		/* Actual stitching into a tree happens here */
		storePath(path);
	}

	private void storePath(FeaturePath<String> path) throws FileSystemException {
		try {
			metadataGraph.addPath(path);
		} catch (Exception e) {
			throw new FileSystemException("Error storing metadata: " + e.getClass().getCanonicalName(), e);
		}
	}

	public MetadataGraph getMetadataGraph() {
		return metadataGraph;
	}

	public List<Path<Feature, String>> query(Query query) {
		return metadataGraph.evaluateQuery(query);
	}

	private List<String[]> getFeaturePaths(String blockPath) throws IOException {
		byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
		String blockData = new String(blockBytes, "UTF-8");
		List<String[]> paths = new ArrayList<String[]>();
		String[] lines = blockData.split("\\r?\\n");
		int splitLimit = this.featureList.size();
		for (String line : lines)
			paths.add(line.split(",", splitLimit));
		return paths;
	}

	private boolean isGridInsidePolygon(GeoavailabilityGrid grid, GeoavailabilityQuery geoQuery) {
		Polygon polygon = new Polygon();
		for (Coordinates coords : geoQuery.getPolygon()) {
			Point<Integer> point = GeoHash.coordinatesToXY(coords);
			polygon.addPoint(point.X(), point.Y());
		}
		
		logger.info("checking geohash " + grid.getBaseHash() + " intersection with the polygon");
		SpatialRange hashRange = grid.getBaseRange();
		Pair<Coordinates, Coordinates> pair = hashRange.get2DCoordinates();
		Point<Integer> upperLeft = GeoHash.coordinatesToXY(pair.a);
		Point<Integer> lowerRight = GeoHash.coordinatesToXY(pair.b);
		
		if (polygon.contains(new Rectangle(upperLeft.X(), upperLeft.Y(), lowerRight.X() - upperLeft.X(),
				lowerRight.Y() - upperLeft.Y())))
			return true;
		return false;
	}

	private class ParallelQueryProcessor implements Runnable {
		private List<String[]> featurePaths;
		private Query query;
		private GeoavailabilityGrid grid;
		private Bitmap queryBitmap;
		private String storagePath;

		public ParallelQueryProcessor(List<String[]> featurePaths, Query query, GeoavailabilityGrid grid,
				Bitmap queryBitmap, String storagePath) {
			this.featurePaths = featurePaths;
			this.query = query;
			this.grid = grid;
			this.queryBitmap = queryBitmap;
			this.storagePath = storagePath + BLOCK_EXTENSION;
		}
		

		@Override
		public void run() {
			try {
				if (queryBitmap != null) {
					int latOrder = -1, lngOrder = -1, index = 0;
					for (Pair<String, FeatureType> columnPair : GeospatialFileSystem.this.featureList) {
						if (columnPair.a.equalsIgnoreCase(GeospatialFileSystem.this.spatialHint.getLatitudeHint()))
							latOrder = index++;
						else if (columnPair.a
								.equalsIgnoreCase(GeospatialFileSystem.this.spatialHint.getLongitudeHint()))
							lngOrder = index++;
						else
							index++;
					}

					GeoavailabilityMap<String[]> geoMap = new GeoavailabilityMap<String[]>(grid);
					Iterator<String[]> pathIterator = this.featurePaths.iterator();
					while (pathIterator.hasNext()) {
						String[] features = pathIterator.next();
						float lat = Math.getFloat(features[latOrder]);
						float lon = Math.getFloat(features[lngOrder]);
						if (!Float.isNaN(lat) && !Float.isNaN(lon))
							geoMap.addPoint(new Coordinates(lat, lon), features);
						pathIterator.remove();
					}
					for (List<String[]> paths : geoMap.query(queryBitmap).values())
						this.featurePaths.addAll(paths);
				}
				if (query != null && this.featurePaths.size() > 0) {
					MetadataGraph temporaryGraph = new MetadataGraph();
					Iterator<String[]> pathIterator = this.featurePaths.iterator();
					while (pathIterator.hasNext()) {
						String[] features = pathIterator.next();
						try {
							Metadata metadata = new Metadata();
							FeatureSet featureset = new FeatureSet();
							for (int i = 0; i < features.length; i++) {
								Pair<String, FeatureType> pair = GeospatialFileSystem.this.featureList.get(i);
								if (pair.b == FeatureType.FLOAT)
									featureset.put(new Feature(pair.a, Math.getFloat(features[i])));
								if (pair.b == FeatureType.INT)
									featureset.put(new Feature(pair.a, Math.getInteger(features[i])));
								if (pair.b == FeatureType.LONG)
									featureset.put(new Feature(pair.a, Math.getLong(features[i])));
								if (pair.b == FeatureType.DOUBLE)
									featureset.put(new Feature(pair.a, Math.getDouble(features[i])));
								if (pair.b == FeatureType.STRING)
									featureset.put(new Feature(pair.a, features[i]));
							}
							metadata.setAttributes(featureset);
							Path<Feature, String> featurePath = createPath("/nopath", metadata);
							temporaryGraph.addPath(featurePath);
						} catch (Exception e) {
							logger.warning(e.getMessage());
						}
						pathIterator.remove();
					}
					List<Path<Feature, String>> evaluatedPaths = temporaryGraph.evaluateQuery(query);
					for (Path<Feature, String> path : evaluatedPaths) {
						String[] featureValues = new String[path.size()];
						int index = 0;
						for (Feature feature : path.getLabels())
							featureValues[index++] = feature.getString();
						this.featurePaths.add(featureValues);
					}
				}
				
				/* THE RESULTS OF THE QUERY GETS WRITTEN TO A LOCAL FILE AND JUST THE PATH TO THAT FILE IS RETURNED FOR SUBSEQUENT FETCHING */
				
				if (featurePaths.size() > 0) {
					try (FileOutputStream fos = new FileOutputStream(this.storagePath)) {
						Iterator<String[]> pathIterator = featurePaths.iterator();
						while (pathIterator.hasNext()) {
							String[] path = pathIterator.next();
							StringBuffer pathSB = new StringBuffer();
							for (int j = 0; j < path.length; j++) {
								pathSB.append(path[j]);
								if (j + 1 != path.length)
									pathSB.append(",");
							}
							fos.write(pathSB.toString().getBytes("UTF-8"));
							pathIterator.remove();
							if (pathIterator.hasNext())
								fos.write("\n".getBytes("UTF-8"));
						}
					}
				} else {
					this.storagePath = null;
				}
				
			} catch (IOException | BitmapException e) {
				logger.log(Level.SEVERE, "Something went wrong while querying the filesystem.", e);
				this.storagePath = null;
			}
		}

		public String getStoragePath() {
			return this.storagePath;
		}

	}

	/**
	 * 
	 * @author sapmitra
	 * @param blockPath
	 * @return
	 * @throws IOException
	 */
	private List<List<String[]>> getFeaturePathsFromBlockSet(List<String> blockPaths, PathFragments fragments) throws IOException {
		
		List<Integer> chunks = new ArrayList<Integer>(fragments.getChunks());
		
		/* Records is all possible 27 chunks + one slot for the full block if only the full block is required */
		List<List<String[]>> records = new ArrayList<List<String[]>>();
		
		for(int i=0; i< 28; i++) {
			records.add(null);
		}
		
		
		/* Reading each blocks that may lie in a path */
		for(String blockPath : blockPaths) {
			
			/* If only the whole block is needed */
			if(fragments.isIgnore()) {
				/* Getting all the records of this particular block */
				List<String[]> record = getFeaturePaths(blockPath);
				
				/*LOGGING*/
				String rr = "";
				for(String[] hh : record) {
					rr += Arrays.toString(hh)+"$$";
				}
				logger.log(Level.INFO, "RIKI: READ RECORDS FOR IGNORE IE FULL BLOCK "+rr + " " +blockPaths);
				if(record != null && record.size() > 0) {
				
					if(records.get(27) == null) {
						
						records.set(27, record);
					} else {
						
						List<String[]> recordOld = records.get(27);
						recordOld.addAll(record);
						//records.set(27, record);
					}
				
				}
				continue;
			}
			
			/* In case we need to process in fragments */
			
			byte[] blockBytes = Files.readAllBytes(Paths.get(blockPath));
			String blockData = new String(blockBytes, "UTF-8");
			String[] lines = blockData.split("\\r?\\n");
			int splitLimit = this.featureList.size();
			
			BorderingProperties borderingProperties = borderMap.get(blockPath);
			//logger.log(Level.INFO, "RIKI: BORDERING PROPERTIES FOUND " +borderingProperties +" "+ blockPaths);
			logger.log(Level.INFO, "RIKI: ABOUT TO READ RECORDS FOR NOT FULL BLOCK " +blockPaths);
			//logger.log(Level.INFO, "RIKI: FEATURELIST SIZE " +splitLimit);
			
			/* Gets the actual records numbers needed in a 28 length list of list representing each fragment*/
			for(int i : chunks) {
				List<Long> recordsToRead = OrientationManager.getRecordNumbersFromBlock(i, borderingProperties);
				//logger.log(Level.INFO, "RIKI: LINE NOS TO READ " + recordsToRead +" "+blockPaths);
				if(recordsToRead == null || recordsToRead.size() == 0)
					continue;
				
				List<String[]> paths = new ArrayList<String[]>();
				
				for (long l : recordsToRead) {
					String line = lines[(int)l];
					paths.add(line.split(",", splitLimit));
				}
				
				if(records.get(i) == null) {
					
					records.set(i, paths);
				} else {
					
					List<String[]> recordOld = records.get(27);
					recordOld.addAll(paths);
					//records.set(27, record);
				}
				
				
				/*LOGGING*/
				String rr = "";
				for(String[] hh : paths) {
					rr += Arrays.toString(hh)+"$$";
				}
				logger.log(Level.INFO, "RIKI: READ RECORDS FOR NOT FULL BLOCK "+ rr + " " +blockPaths);
				
			}
			
		}
		return records;
	}
	
	
	
	
	private List<String[]> getFeaturePathsLocal(List<String> blockPaths) throws IOException {
		
		/* Records is all possible 27 chunks + one slot for the full block if only the full block is required */
		List<String[]> records = new ArrayList<String[]>();
		
		/* Reading each blocks that may lie in a path */
		for(String blockPath : blockPaths) {
			
			/* If only the whole block is needed */
			
			/* Getting all the records of this particular block */
			List<String[]> record = getFeaturePaths(blockPath);
			
			if(record != null && record.size() > 0) {
			
				records.addAll(record);
			
			}
			
		}
		return records;
	}
	
	public List<String> query(String blockPath, GeoavailabilityQuery geoQuery, GeoavailabilityGrid grid,
			Bitmap queryBitmap, String pathPrefix) throws IOException, InterruptedException {
		List<String> resultFiles = new ArrayList<>();
		List<String[]> featurePaths = null;
		boolean skipGridProcessing = false;
		if (geoQuery.getPolygon() != null && geoQuery.getQuery() != null) {
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			
			// THIS READS THE ACTUAL BLOCK
			// Creates a path graph from block data
			featurePaths = getFeaturePaths(blockPath);
		} else if (geoQuery.getPolygon() != null) {
			/* If grid lies completely inside polygon */
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			if (!skipGridProcessing)
				featurePaths = getFeaturePaths(blockPath);
		} else if (geoQuery.getQuery() != null) {
			featurePaths = getFeaturePaths(blockPath);
		} else {
			resultFiles.add(blockPath);
			return resultFiles;
		}

		if (featurePaths == null) {
			resultFiles.add(blockPath);
			return resultFiles;
		}

		queryBitmap = skipGridProcessing ? null : queryBitmap;
		int size = featurePaths.size();
		int partition = java.lang.Math.max(size / numCores, MIN_GRID_POINTS);
		int parallelism = java.lang.Math.min(size / partition, numCores);
		if (parallelism > 1) {
			ExecutorService executor = Executors.newFixedThreadPool(parallelism);
			List<ParallelQueryProcessor> queryProcessors = new ArrayList<>();
			for (int i = 0; i < parallelism; i++) {
				int from = i * partition;
				int to = (i + 1 != parallelism) ? (i + 1) * partition : size;
				List<String[]> subset = new ArrayList<>(featurePaths.subList(from, to));
				ParallelQueryProcessor pqp = new ParallelQueryProcessor(subset, geoQuery.getQuery(), grid, queryBitmap,
						pathPrefix + "-" + i);
				queryProcessors.add(pqp);
				executor.execute(pqp);
			}
			featurePaths.clear();
			executor.shutdown();
			executor.awaitTermination(10, TimeUnit.MINUTES);
			for (ParallelQueryProcessor pqp : queryProcessors)
				if (pqp.getStoragePath() != null)
					resultFiles.add(pqp.getStoragePath());
		} else {
			ParallelQueryProcessor pqp = new ParallelQueryProcessor(featurePaths, geoQuery.getQuery(), grid,
					queryBitmap, pathPrefix);
			pqp.run(); // to avoid another thread creation
			if (pqp.getStoragePath() != null)
				resultFiles.add(pqp.getStoragePath());
		}

		return resultFiles;
	}

	public JSONArray getFeaturesJSON() {
		return metadataGraph.getFeaturesJSON();
	}

	@Override
	public void shutdown() {
		logger.info("FileSystem shutting down");
		try {
			pathJournal.shutdown();
		} catch (Exception e) {
			/* Everything is going down here, just print out the error */
			e.printStackTrace();
		}
	}

	public int getSpatialUncertaintyPrecision() {
		return spatialUncertaintyPrecision;
	}

	public void setSpatialUncertaintyPrecision(int spatialUncertaintyPrecision) {
		this.spatialUncertaintyPrecision = spatialUncertaintyPrecision;
	}

	public int getTemporalUncertaintyPrecision() {
		return temporalUncertaintyPrecision;
	}

	public void setTemporalUncertaintyPrecision(int temporalUncertaintyPrecision) {
		this.temporalUncertaintyPrecision = temporalUncertaintyPrecision;
	}

	public boolean isRasterized() {
		return isRasterized;
	}

	public void setRasterized(boolean isRasterized) {
		this.isRasterized = isRasterized;
	}

	public String getTemporalHint() {
		return temporalHint;
	}

	public void setTemporalHint(String temporalHint) {
		this.temporalHint = temporalHint;
	}
	
	
	public static String getSpatialFeatureName() {
		return SPATIAL_FEATURE;
	}
	
	public static String getTemporalYearFeatureName() {
		return TEMPORAL_YEAR_FEATURE;
	}
	
	public static String getTemporalMonthFeatureName() {
		return TEMPORAL_MONTH_FEATURE;
	}
	public static String getTemporalDayFeatureName() {
		return TEMPORAL_DAY_FEATURE;
	}
	public static String getTemporalHourFeatureName() {
		return TEMPORAL_HOUR_FEATURE;
	}
	
	
	/**
	 * This should return all records that match in a list of list with 28 entries, some of which may be null
	 * 
	 * @author sapmitra
	 * @param blocks : link to all blocks in a path
	 * @param geoQuery
	 * @param grid
	 * @param queryBitmap
	 * @param fragments : specifies the fragments that are needed for this particular path and blocks in it
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public List<String> queryFragments(List<String> blocks, GeoavailabilityQuery geoQuery, GeoavailabilityGrid grid, Bitmap queryBitmap, PathFragments fragments) 
			throws IOException, InterruptedException {
		
		/* Represents all possible fragments. Only the fields representing the required fragments will be populated */
		List<List<String[]>> featurePaths = new ArrayList<List<String[]>>();
		
		/* Each string in this list represents a fragment of a path */
		List<String> recordFragmentsPerPath = new ArrayList<String>();
		
		for(int i=0; i < 28; i++) {
			recordFragmentsPerPath.add("");
			featurePaths.add(null);
			recordFragmentsPerPath.add("");
			
		}
		//logger.info("RIKI: HERE FOR "+ blocks);
		//logger.info("RIKI: PATH FRAGMENTS LATER:"+blocks+" "+fragments);
		// THIS READS THE ACTUAL BLOCKS
		// RETURNS ALL RECORDS FOR EACH FRAGMENT IN A LIST OF 28
		boolean skipGridProcessing = false;
		if (geoQuery.getPolygon() != null && geoQuery.getQuery() != null) {
			/* If polygon complete encompasses geohash */
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			
			featurePaths = getFeaturePathsFromBlockSet(blocks, fragments);
			//logger.info("RIKI: SHOULDNT COME HERE"+blocks);
		} else if (geoQuery.getPolygon() != null) {
			/* If grid lies completely inside polygon */
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			
			featurePaths = getFeaturePathsFromBlockSet(blocks, fragments);
			//logger.info("RIKI: FOUNDPATHS:"+featurePaths + blocks);
		} else if (geoQuery.getQuery() != null) {
			featurePaths = getFeaturePathsFromBlockSet(blocks, fragments);
			//logger.info("RIKI: SHOULDNT COME HERE1"+blocks);
		} 
		
		
		boolean fullyEmpty = true;
		int parallelism = 0;
		
		for(int i=0; i < 28; i++) {
			
			if(featurePaths.get(i) != null) {
				fullyEmpty = false;
				parallelism++;
			}
			
		}
		logger.info("RIKI: FULLYEMPTY:"+fullyEmpty +" "+blocks);
		/* No matching records found. No need to query */
		if(fullyEmpty) {
			return null;
		}
		/* ALL THE RECORDS TO BE QUERIED ARE NOW INSIDE featurePaths */
		
		// FURTHER FILTERING OF featurePaths
		
		queryBitmap = skipGridProcessing ? null : queryBitmap;
		
		if (parallelism > 0) {
			ExecutorService executor = Executors.newFixedThreadPool(parallelism);
			List<NeighborDataParallelQueryProcessor> queryProcessors = new ArrayList<>();
			int i=0;
			for (List<String[]> subset: featurePaths) {
				if(subset != null && subset.size() > 0) {
					logger.log(Level.INFO, "RIKI: INDIVIDUAL SUBSETS "+i+" "+subset + " "+ blocks);
					NeighborDataParallelQueryProcessor pqp = new NeighborDataParallelQueryProcessor(this, subset, geoQuery.getQuery(), grid, queryBitmap, i, blocks);
					
					queryProcessors.add(pqp);
					executor.execute(pqp);
				}
				i++;
			}
			
			executor.shutdown();
			boolean status = executor.awaitTermination(10, TimeUnit.MINUTES);
			if (!status)
				logger.log(Level.WARNING, "queryFragments: Executor terminated because of the specified timeout=10minutes");
			
			fullyEmpty = true;
			for(NeighborDataParallelQueryProcessor nqp : queryProcessors) {
				logger.log(Level.INFO, "RIKI: DID IT ENTER? " + blocks);
				if(nqp.getRecordsStringRepresentation() != null && nqp.getRecordsStringRepresentation().length() > 0) {
					logger.log(Level.INFO, "RIKI: INDIVIDUAL FRAGMENTS "+nqp.getRecordsStringRepresentation() + " "+ blocks);
					fullyEmpty = false;
					int index = nqp.getFragNum();
					recordFragmentsPerPath.add(index, nqp.getRecordsStringRepresentation());
				}
				if(fullyEmpty) {
					return null;
				}
			}
			
		} 

		return recordFragmentsPerPath;
	}
	
	/**
	 * 
	 * @author sapmitra
	 * @param blocks
	 * @param geoQuery
	 * @param grid
	 * @param queryBitmap
	 * @param fragments
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	
	// gets called once per supercube
	public List<String[]> queryLocal(List<String> blocks, GeoavailabilityQuery geoQuery, GeoavailabilityGrid grid, Bitmap queryBitmap) 
			throws IOException, InterruptedException {
		
		List<String[]> featurePaths = new ArrayList<String[]>();
		List<String[]> returnPaths = new ArrayList<String[]>();
		
		// THIS READS THE ACTUAL BLOCKS
		
		boolean skipGridProcessing = false;
		if (geoQuery.getPolygon() != null && geoQuery.getQuery() != null) {
			/* If polygon complete encompasses geohash */
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			
			featurePaths = getFeaturePathsLocal(blocks);
		} else if (geoQuery.getPolygon() != null) {
			/* If grid lies completely inside polygon */
			skipGridProcessing = isGridInsidePolygon(grid, geoQuery);
			featurePaths = getFeaturePathsLocal(blocks);
		} else if (geoQuery.getQuery() != null) {
			featurePaths = getFeaturePathsLocal(blocks);
		} 
		
		logger.log(Level.INFO, "RIKI: FS1 LOCAL RECORDS FOUND: "+Arrays.asList(featurePaths));
		int size = featurePaths.size();
		int partition = java.lang.Math.max(size / numCores, MIN_GRID_POINTS);
		int parallelism = java.lang.Math.min(size / partition, numCores);
		
		/* ALL THE RECORDS TO BE QUERIED ARE NOW INSIDE featurePaths */
		
		// FURTHER FILTERING OF FEATUREPATHS
		
		queryBitmap = skipGridProcessing ? null : queryBitmap;
		if(parallelism <=0 ) {
			logger.log(Level.INFO, "RIKI: THIS HAPPENED");
			parallelism = 1;
		}
		if (parallelism > 0) {
			
			ExecutorService executor = Executors.newFixedThreadPool(parallelism);
			
			List<LocalParallelQueryProcessor> queryProcessors = new ArrayList<>();
			
			for (int i = 0; i < parallelism; i++) {
				int from = i * partition;
				int to = (i + 1 != parallelism) ? (i + 1) * partition : size;
				List<String[]> subset = new ArrayList<>(featurePaths.subList(from, to));
				logger.log(Level.INFO, "RIKI: FS1 LOCAL RECORDS FOUND2: "+Arrays.asList(featurePaths));
				logger.log(Level.INFO, "RIKI: FS1 LOCAL RECORDS FOUND3: "+Arrays.asList(subset));
				if(subset != null) {
					
					LocalParallelQueryProcessor pqp = new LocalParallelQueryProcessor(this, subset, geoQuery.getQuery(), grid, queryBitmap);
					
					queryProcessors.add(pqp);
					executor.execute(pqp);
				}
			}
			
			executor.shutdown();
			boolean status = executor.awaitTermination(10, TimeUnit.MINUTES);
			if (!status)
				logger.log(Level.WARNING, "queryFragments: Executor terminated because of the specified timeout=10minutes");
			
			for(LocalParallelQueryProcessor nqp : queryProcessors) {
				logger.log(Level.INFO, "RIKI: LocalParallelQueryProcessor PATHS6"+nqp.getFeaturePaths());
				if(nqp.getFeaturePaths().size() > 0) {
					returnPaths.addAll(nqp.getFeaturePaths());
				}
				
			}
			
		} 

		return returnPaths;
	}
	
	

	public List<Pair<String, FeatureType>> getFeatureList() {
		return featureList;
	}

	public void setFeatureList(List<Pair<String, FeatureType>> featureList) {
		this.featureList = featureList;
	}

	public SpatialHint getSpatialHint() {
		return spatialHint;
	}

	public void setSpatialHint(SpatialHint spatialHint) {
		this.spatialHint = spatialHint;
	}

	public Map<String, BorderingProperties> getBorderMap() {
		return borderMap;
	}

	public void setBorderMap(Map<String, BorderingProperties> borderMap) {
		this.borderMap = borderMap;
	}
	
	public String getName() {
		return name;
	}

	public int getTemporalPosn() {
		return temporalPosn;
	}

	public void setTemporalPosn(int temporalPosn) {
		this.temporalPosn = temporalPosn;
	}

	public int getSpatialPosn1() {
		return spatialPosn1;
	}

	public void setSpatialPosn1(int spatialPosn1) {
		this.spatialPosn1 = spatialPosn1;
	}

	public int getSpatialPosn2() {
		return spatialPosn2;
	}

	public void setSpatialPosn2(int spatialPosn2) {
		this.spatialPosn2 = spatialPosn2;
	}
}
