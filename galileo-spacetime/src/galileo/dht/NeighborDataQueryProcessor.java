package galileo.dht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.bmp.Bitmap;
import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityQuery;
import galileo.comm.NeighborDataResponse;
import galileo.dataset.feature.Feature;
import galileo.event.EventContext;
import galileo.fs.GeospatialFileSystem;
import galileo.graph.Path;
import galileo.util.PathFragments;

/* This handles a single path */
public class NeighborDataQueryProcessor implements Runnable{
	
	private static final Logger logger = Logger.getLogger("galileo");
	
	/* Represents the path that will be queried. Allblocks in this path will be looked into. */
	private Path<Feature, String> path;
	private List<String> blocks;
	private GeoavailabilityQuery geoQuery;
	private GeoavailabilityGrid grid;
	private GeospatialFileSystem gfs;
	private Bitmap queryBitmap;
	private EventContext context;
	private String nodeString;
	
	/* This contains the actual records returned from the query */
	/* This is a list of 28 strings, some of which may be empty */
	/* Each of these string is a full record list of a fragment of this path */
	private List<String> resultRecordLists;
	private long fileSize;
	private PathFragments pathFragments;
	private int pathIndex;
	private String pathInfo;
	
	public NeighborDataQueryProcessor(GeospatialFileSystem gfs, Path<Feature, String> path, GeoavailabilityQuery gQuery, 
			GeoavailabilityGrid grid, Bitmap queryBitmap, PathFragments pathFragments, EventContext context, int pathIndex, String nodeString) {
		
		this.gfs = gfs;
		this.path = path;
		this.geoQuery = gQuery;
		this.grid = grid;
		this.queryBitmap = queryBitmap;
		this.pathFragments = pathFragments;
		this.blocks = new ArrayList<String>(path.getPayload());
		this.context = context;
		this.pathIndex = pathIndex;
		this.pathInfo = GeospatialFileSystem.getPathInfo(path, 0);
		this.nodeString = nodeString;
		
		/*for(int i=0; i < 28; i++) {
			
			resultPaths.set(i, null);
			
		}*/
	}
	
	@Override
	public void run() {
		
		try {
			/* This thread is created one for each path */
			this.resultRecordLists = this.gfs.queryFragments(this.blocks, this.geoQuery, this.grid, this.queryBitmap, this.pathFragments);
			NeighborDataResponse ndr = createNeighborResponse();
			context.sendReply(ndr);
			logger.info("RIKI: SENT BACK BEIGHBOR DATA RESPONSE");
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			logger.log(Level.SEVERE, "Something went wrong while querying FS2 for neighbor block. No results obtained.\n" + e.getMessage());
		}
		
		
	}

	private NeighborDataResponse createNeighborResponse() {
		NeighborDataResponse ndr = new NeighborDataResponse(resultRecordLists, pathIndex, pathInfo, nodeString);
		//logger.log(Level.INFO, "RIKI: SELECTED RECORDS:" + pathInfo+">>"+resultRecordLists);
		return ndr;
	}

	public List<String> getResultRecordLists() {
		return resultRecordLists;
	}

	public void setResultRecordLists(List<String> resultRecordLists) {
		this.resultRecordLists = resultRecordLists;
	}

	public Path<Feature, String> getPath() {
		return path;
	}

	public void setPath(Path<Feature, String> path) {
		this.path = path;
	}
	
	

}
