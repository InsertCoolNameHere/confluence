package galileo.query;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author sapmitra
 * This houses all the neighboring blocks to be queried by a node(n1) to all other nodes in the cluster.
 * 
 * This keeps note of the blocks that were requested so that once data is returned, we know which is for what.
 */
public class NeighborQuery {
	
	private Map<String, List<RequestedBlocks>> cumulativeRequests;

	public Map<String, List<RequestedBlocks>> getCumulativeRequests() {
		return cumulativeRequests;
	}

	public void setCumulativeRequests(Map<String, List<RequestedBlocks>> cumulativeRequests) {
		this.cumulativeRequests = cumulativeRequests;
	}

}
