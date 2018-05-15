package galileo.dht;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.bmp.Bitmap;
import galileo.bmp.BitmapException;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityMap;
import galileo.bmp.GeoavailabilityQuery;
import galileo.dataset.Coordinates;
import galileo.dataset.Metadata;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.dataset.feature.FeatureType;
import galileo.fs.GeospatialFileSystem;
import galileo.graph.FeaturePath;
import galileo.graph.MetadataGraph;
import galileo.graph.Path;
import galileo.query.Query;
import galileo.util.Pair;
import galileo.util.PathFragments;

/**
 * 
 * @author sapmitra
 *
 */
/* This handles one fragment of a path */
public class NeighborDataParallelQueryProcessor implements Runnable{
	
	private static final Logger logger = Logger.getLogger("galileo");
	
	/* Represents the path that will be queried. Records from all blocks in this path has been loaded here. */
	private List<String[]> featurePaths;
	private Query query;
	private GeoavailabilityGrid grid;
	private Bitmap queryBitmap;
	private GeospatialFileSystem gfs;
	private int fragNum;
	private List<String> blocks;
	
	/* The final records from this fragment will not be in a string array format, rather a full string */
	private String recordsStringRepresentation;
	
	/**
	 * 
	 * @param gfs
	 * @param featurePaths: all necessary records
	 * @param query
	 * @param grid
	 * @param queryBitmap
	 */
	public NeighborDataParallelQueryProcessor(GeospatialFileSystem gfs, List<String[]> featurePaths, Query query, GeoavailabilityGrid grid, Bitmap queryBitmap, int fragNum
			, List<String> blocks) {
		
		this.featurePaths = featurePaths;
		this.query = query;
		this.grid = grid;
		this.queryBitmap = queryBitmap;
		this.gfs = gfs;
		this.fragNum = fragNum;
		this.blocks = blocks;
	}
	
	/**
	 * Using the Feature attributes found in the provided Metadata, a path is
	 * created for insertion into the Metadata Graph.
	 */
	protected FeaturePath<String> createPath(String physicalPath, Metadata meta) {
		FeaturePath<String> path = new FeaturePath<String>(physicalPath, meta.getAttributes().toArray());
		return path;
	}
	
	/**
	 * This handles one fragment
	 */
	@Override
	public void run() {
		
		try{
			logger.info("RIKI: FEATUREPATHS IN SUBSET "+featurePaths.size() +" "+blocks);
			boolean fullRequired = false;
			
			if (queryBitmap != null) {
				fullRequired = true;
				logger.info("RIKI: QUERY BITMAP FOUND "+blocks);
				int latOrder = -1, lngOrder = -1, index = 0;
				for (Pair<String, FeatureType> columnPair : gfs.getFeatureList()) {
					if (columnPair.a.equalsIgnoreCase(gfs.getSpatialHint().getLatitudeHint()))
						latOrder = index++;
					else if (columnPair.a
							.equalsIgnoreCase(gfs.getSpatialHint().getLongitudeHint()))
						lngOrder = index++;
					else
						index++;
				}

				GeoavailabilityMap<String[]> geoMap = new GeoavailabilityMap<String[]>(grid);
				Iterator<String[]> pathIterator = this.featurePaths.iterator();
				while (pathIterator.hasNext()) {
					String[] features = pathIterator.next();
					float lat = Float.valueOf(features[latOrder]);
					float lon = Float.valueOf(features[lngOrder]);
					if (!Float.isNaN(lat) && !Float.isNaN(lon))
						geoMap.addPoint(new Coordinates(lat, lon), features);
					pathIterator.remove();
				}
				recordsStringRepresentation = "";
				StringBuilder sb = new StringBuilder();
				/*each string[] is a line of record*/
				for (List<String[]> paths : geoMap.query(queryBitmap).values()) {
					/* No need to update featurePaths if the remaining section will not be evaluated */
					if (query != null && this.featurePaths.size() > 0) {
						this.featurePaths.addAll(paths);
						continue;
					} else if(paths != null && paths.size() > 0){
						for(String[] record : paths) {
							if(record != null) {
								String recStr = Arrays.toString(record);
								//recordsStringRepresentation += recStr.substring(1,recStr.length() - 1) + "\n";
								sb.append(recStr.substring(1,recStr.length() - 1) + "\n");
							}
						}
						recordsStringRepresentation = sb.toString();
					}
						
					
				}
			}
			if (query != null && this.featurePaths.size() > 0) {
				logger.info("RIKI: SHOULD NOT ENTER "+blocks);
				fullRequired = true;
				MetadataGraph temporaryGraph = new MetadataGraph();
				Iterator<String[]> pathIterator = this.featurePaths.iterator();
				while (pathIterator.hasNext()) {
					String[] features = pathIterator.next();
					try {
						Metadata metadata = new Metadata();
						FeatureSet featureset = new FeatureSet();
						for (int i = 0; i < features.length; i++) {
							Pair<String, FeatureType> pair = gfs.getFeatureList().get(i);
							if (pair.b == FeatureType.FLOAT)
								featureset.put(new Feature(pair.a, Float.valueOf(features[i])));
							if (pair.b == FeatureType.INT)
								featureset.put(new Feature(pair.a, Integer.valueOf(features[i])));
							if (pair.b == FeatureType.LONG)
								featureset.put(new Feature(pair.a, Long.valueOf(features[i])));
							if (pair.b == FeatureType.DOUBLE)
								featureset.put(new Feature(pair.a, Double.valueOf(features[i])));
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
				recordsStringRepresentation = "";
				StringBuilder sb = new StringBuilder();
				List<Path<Feature, String>> evaluatedPaths = temporaryGraph.evaluateQuery(query);
				for (Path<Feature, String> path : evaluatedPaths) {
					String[] featureValues = new String[path.size()];
					int index = 0;
					for (Feature feature : path.getLabels())
						featureValues[index++] = feature.getString();
					String recStr = Arrays.toString(featureValues);
					//recordsStringRepresentation += recStr.substring(1,recStr.length() - 1) + "\n";
					sb.append(recStr.substring(1,recStr.length() - 1) + "\n");
					//this.featurePaths.add(featureValues);
				}
				recordsStringRepresentation = sb.toString();
			} 
			//logger.info("RIKI: SHOULD ENTER "+blocks + fullRequired + " "+ this.featurePaths.size() + ">>"+recordsStringRepresentation+"<<");
			
			if(!fullRequired && this.featurePaths.size() > 0 && (recordsStringRepresentation == null || recordsStringRepresentation.isEmpty())) {
				
				recordsStringRepresentation = "";
				StringBuilder sb = new StringBuilder();
				for(String[] record : featurePaths) {
					if(record != null) {
						String recStr = Arrays.toString(record);
						//recordsStringRepresentation += recStr.substring(1,recStr.length() - 1) + "\n";
						sb.append(recStr.substring(1,recStr.length() - 1) + "\n");
					}
				}
				recordsStringRepresentation = sb.toString();
				
				//logger.info("RIKI: SHOULD ENTER AFTER "+ recordsStringRepresentation + " "+blocks);
			}

			/*if (featurePaths.size() > 0) {
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
			}*/
		} catch (BitmapException e) {
			logger.log(Level.SEVERE, "Something went wrong while querying the filesystem.", e);
		}
	
		
		
	}

	public List<String[]> getFeaturePaths() {
		return featurePaths;
	}

	public void setFeaturePaths(List<String[]> featurePaths) {
		this.featurePaths = featurePaths;
	}

	public int getFragNum() {
		return fragNum;
	}

	public void setFragNum(int fragNum) {
		this.fragNum = fragNum;
	}

	public String getRecordsStringRepresentation() {
		return recordsStringRepresentation;
	}

	public void setRecordsStringRepresentation(String recordsStringRepresentation) {
		this.recordsStringRepresentation = recordsStringRepresentation;
	}
	
	

}
