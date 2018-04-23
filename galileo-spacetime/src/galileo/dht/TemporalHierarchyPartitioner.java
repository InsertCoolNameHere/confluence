package galileo.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import galileo.comm.TemporalType;
import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.SpatialRange;
import galileo.dataset.TemporalProperties;
import galileo.dht.hash.BalancedHashRing;
import galileo.dht.hash.ConstrainedGeohash;
import galileo.dht.hash.HashException;
import galileo.dht.hash.HashRing;
import galileo.dht.hash.HashTopologyException;
import galileo.dht.hash.TemporalHash;
import galileo.util.GeoHash;

public class TemporalHierarchyPartitioner extends Partitioner<Metadata> {

	private static final Logger logger = Logger.getLogger("galileo");

	private TemporalHash groupHash;
	private BalancedHashRing<Metadata> groupHashRing;
	private Map<BigInteger, GroupInfo> groupPositions;

	private ConstrainedGeohash nodeHash;
	private Map<BigInteger, BalancedHashRing<Metadata>> nodeHashRings;
	private Map<BigInteger, Map<BigInteger, NodeInfo>> nodePositions;
	
	public TemporalHierarchyPartitioner(StorageNode storageNode, NetworkInfo network, int temporalHashType, int spatialHashType)
			throws PartitionException, HashException, HashTopologyException {

		super(storageNode, network);

		List<GroupInfo> groups = network.getGroups();

		if (groups.size() == 0) {
			throw new PartitionException("At least one group must exist in "
					+ "the current network configuration to use this " + "partitioner.");
		}
		String[] geohashes = null;
		/*String[] geohashes_2char = { "8g", "8u", "8v", "8x", "8y", "8z", "94", "95", "96", "97", "9d", "9e", "9g", "9h", "9j",
				"9k", "9m", "9n", "9p", "9q", "9r", "9s", "9t", "9u", "9v", "9w", "9x", "9y", "9z", "b8", "b9", "bb",
				"bc", "bf", "c0", "c1", "c2", "c3", "c4", "c6", "c8", "c9", "cb", "cc", "cd", "cf", "d4", "d5", "d6",
				"d7", "dd", "de", "dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt", "dw", "dx", "dz", "f0",
				"f1", "f2", "f3", "f4", "f6", "f8", "f9", "fb", "fc", "fd", "ff" };*/
		
		//String[] geohashes_2char = {"9q","9r","9w","9x","9y","9z","dn","dp","dq","dr"};
		String[] geohashes_2char = { "9w", "9x", "9y", "9z"};
		//String[] geohashes_2char = {"9r", "9x","9z", "9y", "9w", "9q", "9y","dp", "dn","9t","9v"};
		Arrays.sort(geohashes_2char);
		
		if(spatialHashType == 2) {
			// Geohashes for US region.
			
			geohashes = geohashes_2char;
		} else if(spatialHashType > 2) {
			
			geohashes = generateGeohashes(geohashes_2char, spatialHashType);
		} else {
			logger.severe("GEOHASH PARTITIONING FAILED. INVALID LENGTH: "+spatialHashType);
		}

		Arrays.sort(geohashes);
		groupHash = new TemporalHash(TemporalType.fromType(temporalHashType));
		groupHashRing = new BalancedHashRing<>(groupHash);
		groupPositions = new HashMap<>();
		nodeHash = new ConstrainedGeohash(geohashes);
		nodeHashRings = new HashMap<>();
		nodePositions = new HashMap<>();
		for (GroupInfo group : groups) {
			placeGroup(group);
		}
	}
	
	public static void main(String arg[]) {
		
		String[] geohashes_2char = { "8u", "8g"};
		String[] generateGeohashes = generateGeohashes(geohashes_2char, 3);
		Arrays.sort(generateGeohashes);
		System.out.println(Arrays.asList(generateGeohashes));
	}

	private static String[] generateGeohashes(String[] geohashes_2char, int spatialHashType) {
		List<String> allGeoHashes = new ArrayList<String>(Arrays.asList(geohashes_2char));
		
		for(int i = 2; i < spatialHashType; i++) {
			
			List<String> currentGeohashes = new ArrayList<String>();
			
			for(String geoHash : allGeoHashes) {
				
				
				SpatialRange range1 = GeoHash.decodeHash(geoHash);
				
				Coordinates c1 = new Coordinates(range1.getLowerBoundForLatitude(), range1.getLowerBoundForLongitude());
				Coordinates c2 = new Coordinates(range1.getUpperBoundForLatitude(), range1.getLowerBoundForLongitude());
				Coordinates c3 = new Coordinates(range1.getUpperBoundForLatitude(), range1.getUpperBoundForLongitude());
				Coordinates c4 = new Coordinates(range1.getLowerBoundForLatitude(), range1.getUpperBoundForLongitude());
				
				ArrayList<Coordinates> cs1 = new ArrayList<Coordinates>();
				cs1.add(c1);cs1.add(c2);cs1.add(c3);cs1.add(c4);
				
				currentGeohashes.addAll(Arrays.asList(GeoHash.getIntersectingGeohashesForConvexBoundingPolygon(cs1, i+1)));
				
			}
			allGeoHashes = currentGeohashes;
			
		}
		Collections.shuffle(allGeoHashes);
		String[] returnArray = allGeoHashes.toArray(new String[allGeoHashes.size()]);
		return returnArray;
	}

	private void placeGroup(GroupInfo group) throws HashException, HashTopologyException {
		BigInteger position = groupHashRing.addNode(null);
		groupPositions.put(position, group);
		logger.info(String.format("Group '%s' placed at %d", group.getName(), position));

		nodeHashRings.put(position, new BalancedHashRing<>(nodeHash));
		for (NodeInfo node : group.getNodes()) {
			placeNode(position, node);
		}
	}

	private void placeNode(BigInteger groupPosition, NodeInfo node) throws HashException, HashTopologyException {
		BalancedHashRing<Metadata> hashRing = nodeHashRings.get(groupPosition);
		BigInteger nodePosition = hashRing.addNode(null);

		GroupInfo group = groupPositions.get(groupPosition);

		logger.info(String.format("Node [%s] placed in Group '%s' at %d", node, group.getName(), nodePosition));

		if (nodePositions.get(groupPosition) == null) {
			nodePositions.put(groupPosition, new HashMap<BigInteger, NodeInfo>());
		}
		nodePositions.get(groupPosition).put(nodePosition, node);
	}

	@Override
	public NodeInfo locateData(Metadata metadata) throws HashException, PartitionException {
		/* First, determine the group that should hold this file */
		BigInteger groupPosition = groupHashRing.locate(metadata);

		HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
		BigInteger node = nodeRing.locate(metadata);
		NodeInfo info = nodePositions.get(groupPosition).get(node);
		if (info == null) {
			throw new PartitionException("Could not locate specified data");
		}
		return info;
	}

	/* Returns a list of nodes that match the query criteria */
	
	public List<NodeInfo> findDestinations(Metadata data) throws HashException {
		/* Since no restrictions, all nodes returned */
		if (data == null)
			return network.getAllNodes();

		TemporalProperties tp = data.getTemporalProperties();
		SpatialProperties sp = data.getSpatialProperties();
		Set<NodeInfo> destinations = new HashSet<NodeInfo>();
		if (tp == null) {
			/* Since no temporal restrictions, all group positions returned */
			Set<BigInteger> positions = groupHashRing.getPositions();
			/* If no spatial positions either, return all nodes */
			if (sp == null) {
				return network.getAllNodes();
			} else {
				if (sp.hasRange() && sp.getSpatialRange().hasPolygon()) {
					List<Coordinates> polygon = sp.getSpatialRange().getPolygon();
					//Spatial range
					logger.info("Polygon - " + polygon);
					// Geohash precision for spatial ring is 2.
					String[] hashes = GeoHash.getIntersectingGeohashes(polygon);
					logger.info("intersecting geohashes - " + Arrays.toString(hashes));
					Metadata metadata = new Metadata();
					for (BigInteger position : positions) {
						HashRing<Metadata> nodeRing = nodeHashRings.get(position);
						for (String hash : hashes) {
							metadata.setSpatialProperties(new SpatialProperties(GeoHash.decodeHash(hash)));
							BigInteger node = nodeRing.locate(metadata);
							destinations.add(nodePositions.get(position).get(node));
						}
					}
				} else {
					for (BigInteger position : positions) {
						HashRing<Metadata> nodeRing = nodeHashRings.get(position);
						BigInteger node = nodeRing.locate(data);
						destinations.add(nodePositions.get(position).get(node));
					}
				}
			}
		} else {
			/* Get the group position based on the temporal metadata */
			
			BigInteger groupPosition = groupHashRing.locate(data);
			if (sp == null) {
				HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
				Set<BigInteger> npositions = nodeRing.getPositions();
				for (BigInteger nposition : npositions)
					destinations.add(nodePositions.get(groupPosition).get(nposition));
			} else {
				HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
				if (sp.hasRange() && sp.getSpatialRange().hasPolygon()) {
					List<Coordinates> polygon = sp.getSpatialRange().getPolygon();
					// Returns a list of 2 character geohashes that have intersection with this Polygon
					String[] hashes = GeoHash.getIntersectingGeohashes(polygon);
					Metadata metadata = new Metadata();
					for (String hash : hashes) {
						metadata.setSpatialProperties(new SpatialProperties(GeoHash.decodeHash(hash)));
						BigInteger node = nodeRing.locate(metadata);
						destinations.add(nodePositions.get(groupPosition).get(node));
					}
				} else {
					BigInteger node = nodeRing.locate(data);
					destinations.add(nodePositions.get(groupPosition).get(node));
				}
			}
		}
		return new ArrayList<NodeInfo>(destinations);
	}
	
	
	/**
	 * 
	 * @author sapmitra
	 * @param data
	 * @return
	 * @throws HashException
	 */
	@Override
	public List<NodeInfo> findDestinationsForFS2(SpatialProperties sps, List<TemporalProperties> tprops, int fs2GeoHashPrecision, String[] validNeighbors) throws HashException, PartitionException {
		
		if (sps == null && tprops == null)
			return network.getAllNodes();
		
		List<Metadata> metas = new ArrayList<Metadata>();
		if (tprops != null) {
			
			for (TemporalProperties tp : tprops) {

				Metadata fs2NodeFindMetadata = new Metadata();

				fs2NodeFindMetadata.setSpatialProperties(sps);
				fs2NodeFindMetadata.setTemporalProperties(tp);
				metas.add(fs2NodeFindMetadata);

			}
		} else {
			
			Metadata fs2NodeFindMetadata = new Metadata();

			fs2NodeFindMetadata.setSpatialProperties(sps);
			metas.add(fs2NodeFindMetadata);
		}
		
		Set<NodeInfo> destinations = new HashSet<NodeInfo>();
		for (Metadata data : metas) {
			TemporalProperties tp = data.getTemporalProperties();
			SpatialProperties sp = data.getSpatialProperties();
			
			if (tp == null) {
				/*
				 * Since no temporal restrictions, all group positions returned
				 */
				Set<BigInteger> positions = groupHashRing.getPositions();
				/* If no spatial positions either, return all nodes */
				if (sp == null) {
					return network.getAllNodes();
				} else {
					if (sp.hasRange() && sp.getSpatialRange().hasPolygon()) {
						List<Coordinates> polygon = sp.getSpatialRange().getPolygon();
						// Spatial range
						logger.info("Polygon - " + polygon);
						
						// There are geohashes from fs2 that matched fs1 geohash requirements
						String[] hashes = GeoHash.getIntersectingGeohashes(polygon, fs2GeoHashPrecision);
						hashes = GeoHash.filterUnwantedGeohashes(hashes, validNeighbors);
						logger.info("intersecting geohashes - " + Arrays.toString(hashes));
						Metadata metadata = new Metadata();
						for (BigInteger position : positions) {
							HashRing<Metadata> nodeRing = nodeHashRings.get(position);
							for (String hash : hashes) {
								metadata.setSpatialProperties(new SpatialProperties(GeoHash.decodeHash(hash)));
								BigInteger node = nodeRing.locate(metadata);
								destinations.add(nodePositions.get(position).get(node));
							}
						}
					} else {
						for (BigInteger position : positions) {
							HashRing<Metadata> nodeRing = nodeHashRings.get(position);
							BigInteger node = nodeRing.locate(data);
							destinations.add(nodePositions.get(position).get(node));
						}
					}
				}
			} else {
				/* Get the group position based on the temporal metadata */

				BigInteger groupPosition = groupHashRing.locate(data);
				if (sp == null) {
					HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
					Set<BigInteger> npositions = nodeRing.getPositions();
					for (BigInteger nposition : npositions)
						destinations.add(nodePositions.get(groupPosition).get(nposition));
				} else {
					HashRing<Metadata> nodeRing = nodeHashRings.get(groupPosition);
					if (sp.hasRange() && sp.getSpatialRange().hasPolygon()) {
						List<Coordinates> polygon = sp.getSpatialRange().getPolygon();
						// Returns a list of 2 character geohashes that have
						// intersection with this Polygon
						String[] hashes = GeoHash.getIntersectingGeohashes(polygon, fs2GeoHashPrecision);
						hashes = GeoHash.filterUnwantedGeohashes(hashes, validNeighbors);
						Metadata metadata = new Metadata();
						for (String hash : hashes) {
							metadata.setSpatialProperties(new SpatialProperties(GeoHash.decodeHash(hash)));
							BigInteger node = nodeRing.locate(metadata);
							destinations.add(nodePositions.get(groupPosition).get(node));
						}
					} else {
						BigInteger node = nodeRing.locate(data);
						destinations.add(nodePositions.get(groupPosition).get(node));
					}
				}
			}
		}
		return new ArrayList<NodeInfo>(destinations);
	}
	
	

	
}
