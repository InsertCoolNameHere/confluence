package galileo.query;

import galileo.dataset.Metadata;

public class RequestedBlocks {
	
	private String blockID;
	private Metadata queryMetadata;
	
	public String getBlockID() {
		return blockID;
	}
	public void setBlockID(String blockID) {
		this.blockID = blockID;
	}
	public Metadata getQueryMetadata() {
		return queryMetadata;
	}
	public void setQueryMetadata(Metadata queryMetadata) {
		this.queryMetadata = queryMetadata;
	}
	

}
