package galileo.dht;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class RikiTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<NodeInfo> nodes = new ArrayList<NodeInfo>();
		
		NodeInfo n1 = new NodeInfo("127.0.0.1",123);
		NodeInfo n2 = new NodeInfo("127.0.0.1",123);
		nodes.add(n1);
		nodes.add(n2);
		
		System.out.println(nodes);
		
		Set<NodeInfo> setNodes = new TreeSet<NodeInfo>(nodes);
		nodes = new ArrayList<NodeInfo>(setNodes);
		
		System.out.println(nodes);

	}

}
