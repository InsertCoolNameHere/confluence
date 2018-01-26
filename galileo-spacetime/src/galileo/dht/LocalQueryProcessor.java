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
public class LocalQueryProcessor implements Runnable{
	
	private static final Logger logger = Logger.getLogger("galileo");
	
	/* Represents the path that will be queried. Allblocks in this path will be looked into. */
	//private Path<Feature, String> path;
	private List<String> blocks;
	private GeoavailabilityQuery geoQuery;
	private GeoavailabilityGrid grid;
	private GeospatialFileSystem fs1;
	private Bitmap queryBitmap;
	private int superCubeId;
	//private EventContext context;
	//private String nodeString;
	
	
	private List<String[]> resultRecordLists;
	//private PathFragments pathFragments;
	//private int pathIndex;
	//private String pathInfo;
	
	/*public LocalQueryProcessor(GeospatialFileSystem gfs, Path<Feature, String> path, GeoavailabilityQuery gQuery, 
			GeoavailabilityGrid grid, Bitmap queryBitmap, PathFragments pathFragments, EventContext context, int pathIndex, String nodeString) {
		
		this.fs1 = gfs;
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
		
		for(int i=0; i < 28; i++) {
			
			resultPaths.set(i, null);
			
		}
	}*/
	
	public LocalQueryProcessor(GeospatialFileSystem fs1, List<String> blocks, GeoavailabilityQuery gQuery,
			GeoavailabilityGrid grid, Bitmap queryBitmap, int superCubeId) {
		
		this.fs1 = fs1;
		this.geoQuery = gQuery;
		this.grid = grid;
		this.queryBitmap = queryBitmap;
		this.blocks = blocks;
		this.superCubeId = superCubeId;
		
	}

	@Override
	public void run() {
		
		try {
			
			/* This thread is created one for each path */
			this.resultRecordLists = this.fs1.queryLocal(this.blocks, this.geoQuery, this.grid, this.queryBitmap);
			
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			logger.log(Level.SEVERE, "Something went wrong while querying FS2 for neighbor block. No results obtained.\n" + e.getMessage());
		}
		
		
	}

	public List<String[]> getResultRecordLists() {
		return resultRecordLists;
	}

	public void setResultRecordLists(List<String[]> resultRecordLists) {
		this.resultRecordLists = resultRecordLists;
	}

	public int getSuperCubeId() {
		return superCubeId;
	}

	public void setSuperCubeId(int superCubeId) {
		this.superCubeId = superCubeId;
	}

}
