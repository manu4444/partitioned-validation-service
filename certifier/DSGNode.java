package certifier;

import java.util.*;

public class DSGNode {
	long ts;
	LinkedList<Long> outEdges;
	
	public DSGNode(long ts){
		this.ts = ts;
		outEdges = new LinkedList<Long>();
	}
	
	public void addEdge(long outTS){
		outEdges.add(outTS);
	}
	
	public void addEdges(Collection<Long> edgeList){
		for(Long edge: edgeList){
			outEdges.add(edge);
		}
	}
	
	public void removeEdge(long outTS) {
		outEdges.remove(outTS);
	}

	public long getTs() {
		return ts;
	}

	public LinkedList<Long> getOutEdges() {
		return outEdges;
	}
	
	
}
