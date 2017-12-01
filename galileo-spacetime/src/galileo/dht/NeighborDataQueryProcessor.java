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
import galileo.dataset.feature.Feature;
import galileo.fs.GeospatialFileSystem;
import galileo.graph.Path;
import galileo.util.PathFragments;

public class NeighborDataQueryProcessor implements Runnable{
	
	private static final Logger logger = Logger.getLogger("galileo");
	
	/* Represents the path that will be queried. Allblocks in this path will be looked into. */
	private Path<Feature, String> path;
	private List<String> blocks;
	private GeoavailabilityQuery geoQuery;
	private GeoavailabilityGrid grid;
	private GeospatialFileSystem gfs;
	private Bitmap queryBitmap;
	private List<List<String>> resultPaths;
	private long fileSize;
	private PathFragments pathFragments;
	
	public NeighborDataQueryProcessor(GeospatialFileSystem gfs, Path<Feature, String> path, GeoavailabilityQuery gQuery, GeoavailabilityGrid grid, Bitmap queryBitmap, PathFragments pathFragments) {
		
		this.gfs = gfs;
		this.path = path;
		this.geoQuery = gQuery;
		this.grid = grid;
		this.queryBitmap = queryBitmap;
		this.pathFragments = pathFragments;
		this.blocks = new ArrayList<String>(path.getPayload());
		
		/*for(int i=0; i < 28; i++) {
			
			resultPaths.set(i, null);
			
		}*/
	}
	
	@Override
	public void run() {
		
		try {
			this.resultPaths = this.gfs.queryFragments(this.blocks, this.geoQuery, this.grid, this.queryBitmap, this.pathFragments);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			logger.log(Level.SEVERE, "Something went wrong while querying FS2 for neighbor block. No results obtained.\n" + e.getMessage());
		}
		
		
	}
	
	

}
