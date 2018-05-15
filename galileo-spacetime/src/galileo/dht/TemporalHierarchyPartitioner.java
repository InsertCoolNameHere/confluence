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
		//String[] geohashes_2char = { "9w", "9x", "9y", "9z"};
		
		/*String[] geohashes_2char = {"b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","c0","c1","c2","c3","c4","c5","c6","c7"
				,"c8","c9","bb","bc","bd","be","bf","bg","bh","bk","d0","bn","d1","d2","d3","d4","d5","bs","d6","d7","bu","d8"
				,"d9","bz","cb","cc","cd","ce","cf","cg","ch","ck","cm","e0","e1","cp","e2","cq","e3","cr","cs","e5","e6","ct"
				,"cu","e7","cv","e9","cw","cx","cy","cz","db","dc","dd","de","df","dg","dh","dj","dk","dm","dn","f0","f1","dp"
				,"f2","dq","f3","dr","f4","f5","ds","f6","dt","du","f7","f8","dv","dw","f9","dx","dy","dz","eb","ec","ed","ee"
				,"ef","eh","ej","ek","em","g0","en","g1","ep","g2","eq","g3","g4","er","g5","es","et","g6","g7","eu","ev","g8"
				,"g9","ew","ex","ey","ez","fb","fc","fd","fe","ff","fg","fh","fj","fk","fm","fn","fp","fr","fs","h5","ft","fu"
				,"h7","fv","fw","fx","fz","gb","gc","gd","ge","gf","gg","gh","gk","gn","gp","gq","gr","gs","gt","gu","gv","gw"
				,"gx","gy","hf","hg","j1","j5","j7","j9","hz","k1","k2","k3","k6","k7","k8","k9","jb","jd","je","jg","jp","jw"
				,"jx","kb","kd","ke","kg","kk","km","m0","kn","m2","kp","kq","kr","ks","m5","kt","ku","kv","kw","m9","kx","ky"
				,"kz","n3","n6","mh","mj","mk","mm","mn","mp","mq","mr","mv","mw","mx","my","mz","nd","nh","nk","p4","ns","p7"
				,"p9","q7","q9","pc","pd","pe","pf","pg","ph","r0","r1","r2","r3","r4","r5","r6","r7","pw","px","py","pz","qc"
				,"qd","qe","qf","qg","qj","qm","s0","s1","s2","qp","qq","s3","s4","qr","12","s5","qs","13","14","s6","qt","qu"
				,"s7","qv","s8","qw","s9","qx","qy","qz","rb","rc","rd","re","0c","rh","0f","rj","rk","rm","rn","t0","rp","20"
				,"rq","t4","rr","t5","rs","rt","24","ru","t7","t8","rv","t9","rw","rx","ry","rz","sb","sc","sd","se","sf","sg"
				,"sh","sj","sk","sm","sn","u0","u1","sp","u2","u3","sq","sr","u4","u5","ss","u6","st","u7","su","35","u8","sv"
				,"u9","sw","sx","sy","sz","tb","tc","td","te","tf","tg","2e","th","2g","tj","2h","tk","2j","2k","tm","v0","tn"
				,"v1","2n","v2","tp","tq","v3","v4","2p","tr","v5","ts","2q","tt","v6","tu","v7","2s","v8","tv","2t","tw","v9"
				,"2u","47","tx","2v","ty","tz","2y","ub","uc","ud","ue","uf","ug","3e","uh","uj","uk","um","w0","un","w1","w2"
				,"w3","uq","w4","3p","w5","us","w6","ut","54","w7","uu","w8","56","w9","uw","3u","59","3x","3z","vb","vc","vd"
				,"ve","vf","vg","4e","vh","vk","x0","x1","4m","x2","x3","vq","62","x4","4q","vs","63","x5","4r","64","vt","vu"
				,"4s","x7","x8","66","vv","67","x9","vw","68","69","4w","4x","wb","wc","wd","5b","we","wf","wg","5e","wh","5f"
				,"5g","wj","wk","5j","wm","y0","wn","y1","y2","wp","5n","y3","wq","y4","wr","ws","y5","wt","y6","75","y7","wu"
				,"wv","y8","y9","ww","wx","wy","79","wz","xb","xc","xe","6c","6d","xf","6e","6f","xh","6g","xj","6h","xk","xm"
				,"6k","xn","z0","6m","z1","xp","80","z2","6n","z3","xq","6p","z4","xr","82","83","z5","6q","xs","z6","84","6r"
				,"xt","6s","z7","85","6t","z8","86","xv","z9","6u","xw","87","6v","xx","88","89","xy","6w","xz","6x","6y","6z"
				,"yb","yc","yd","7b","ye","yf","yg","yh","7h","yk","7j","ym","7k","yn","7n","91","yq","7p","93","ys","7q","yt"
				,"94","7r","yu","95","96","yv","7v","7w","yy","7y","7z","zb","zc","zd","8b","8c","ze","zf","8d","zg","8e","8f"
				,"zh","8g","zj","8h","zk","8j","8k","zm","8m","8n","8p","zs","zu","8s","8t","8u","8v","8w","zy","8x","8y","8z"
				,"9b","9d","9e","9f","9g","9h","9j","9m","9n","9p","9q","9r","9s","9t","9u","9v","9w","9x","9y","9z"};
		*/
		String[] geohashes_2char = {"b","c","f","g","u","v","y","z",
									"8","9","d","e","s","t","w","x",
									"2","3","6","7","k","m","q","r",
									"0","1","4","5","h","j","n","p"};
		
		Arrays.sort(geohashes_2char);
		
		if(spatialHashType == 1) {
			// Geohashes for US region.
			
			geohashes = geohashes_2char;
		} else if(spatialHashType > 1) {
			
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
