package certifier;

import java.util.*;

import org.apache.hadoop.hbase.util.*;

import xact.Dependency;
import util.*;
//import xact.TransactionHandler;

public class DependencyManager {
	//set { row-id => [ list of < version timestamps, list of <reader timestamps> > ]}
	//Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>> rwTable;
	
	Hashtable<String, Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>>> rwMap; 
	Hashtable<Long, DSGNode> dsgGraph;
	
	
	
	public DependencyManager() {
		rwMap = new Hashtable<String, Hashtable<byte[], LinkedList<Pair<Long,LinkedList<Long>>>>>();
		dsgGraph = new Hashtable<Long, DSGNode>();
		
	}	
	
	public boolean checkWWConflict(Collection<byte[]> writeSet, long startTS,
			Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>> singleTableMap) {
		boolean conflict = false;
		for( byte[] w: writeSet) {
			if(singleTableMap.containsKey(w)) {
				LinkedList< Pair< Long, LinkedList<Long>> > rwList = singleTableMap.get(w);
				if (rwList.getLast().getFirst().longValue() > startTS) {
					//concurrent committed transaction, i.e. transaction with commit ts > given start ts
					//has written to this item, so conflict
					conflict = true;
					break;
				}
			}
		}
		return conflict;
	}
	
	public boolean checkWWConflict(Hashtable<String,Pair<Collection<byte[]>, Collection<byte[]>>> rwSet, long startTS){
		boolean conflict = false;
		for(String table: rwSet.keySet()){
			Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>> singleTableMap = null;
			if(!rwMap.containsKey(table)){
				 singleTableMap = new Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>>();
			}else {			
				singleTableMap = rwMap.get(table);
			}
			Collection<byte[]> wset = rwSet.get(table).getSecond();
			conflict = checkWWConflict(wset, startTS, singleTableMap);
			if(conflict) {				
				break;
			}
		}
		return conflict;
	}
	
	public void addDependencies(Hashtable<String,Pair<Collection<byte[]>, Collection<byte[]>>> rwSet,
			long startTS, long commitTS) { 
		for(String table: rwSet.keySet()) {			
			Collection<byte[]> rset = rwSet.get(table).getFirst();
			Collection<byte[]> wset = rwSet.get(table).getSecond();
			addDependencies(rset,wset,table,startTS,commitTS);
		}
	}
	
	public void addWrites( Hashtable<String,Pair<Collection<byte[]>, Collection<byte[]>>> rwSet,
			long startTS, long commitTS) { 
		for(String table: rwSet.keySet()) {			
			Collection<byte[]> wset = rwSet.get(table).getSecond();
			addWrites(wset, table, startTS, commitTS);
		}
	}
	
	//should be called only by CertifierRequestHandler.checkSI()
	public void addWrites(Collection<byte[]> writeSet,String tableName, long startTS, long commitTS) {
		Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>> singleTableMap = rwMap.get(tableName);
		if(singleTableMap == null){
			singleTableMap = new Hashtable<byte[], LinkedList<Pair<Long,LinkedList<Long>>>>();
		}
		for(byte[] w: writeSet){
			if(singleTableMap.containsKey(w)){
				LinkedList< Pair< Long, LinkedList<Long>>> rwList = singleTableMap.get(w);				
				//add this version info 
				Pair<Long, LinkedList<Long>> versionInfo = new Pair<Long, LinkedList<Long>>();
				versionInfo.setFirst(commitTS);
				versionInfo.setSecond(new LinkedList<Long>());
				rwList.add(versionInfo);
				//update rwtable
				singleTableMap.put(w, rwList);
			}else{
				//add this version info 
				LinkedList< Pair< Long, LinkedList<Long>>> rwList = new LinkedList<Pair<Long,LinkedList<Long>>>();
				Pair<Long, LinkedList<Long>> versionInfo = new Pair<Long, LinkedList<Long>>();
				versionInfo.setFirst(commitTS);
				versionInfo.setSecond(new LinkedList<Long>());
				rwList.add(versionInfo);
				//update rwtable
				singleTableMap.put(w, rwList);
			}
		}
	}
	public void addDependencies(Collection< byte[]> readSet, Collection<byte[]> writeSet,
			String tableName, long startTS, long commitTS) {
		Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>> singleTableMap = rwMap.get(tableName);
		if(singleTableMap == null){
			singleTableMap = new Hashtable<byte[], LinkedList<Pair<Long,LinkedList<Long>>>>();
		}
		for(byte[] r: readSet){
			if(singleTableMap.containsKey(r)){
				LinkedList< Pair< Long, LinkedList<Long>>> rwList = singleTableMap.get(r);
				for(int i=rwList.size()-1; i>= 0; i--){
					Pair<Long, LinkedList<Long>> versionInfo = rwList.get(i);
					long writerTS = versionInfo.getFirst() ;
					if(writerTS <= startTS){ 
						//this is the version read						
						//add this txn's timestamp to the list of readers
						LinkedList<Long> readerList = versionInfo.getSecond();
						readerList.add(commitTS);	
						versionInfo.setSecond(readerList);
						//update rwList for this object
						rwList.remove(i);
						rwList.add(i, versionInfo);
						//update the rwTable
						singleTableMap.put(r, rwList);
						//this is wr dependency 
						//add the dependency to graph: writerTS -> commitTS 
						if(dsgGraph.containsKey(writerTS)){
							DSGNode node = dsgGraph.get(writerTS);
							if (!node.getOutEdges().contains(commitTS)){
								node.addEdge(commitTS);
								dsgGraph.put(writerTS, node);
							}
						}else{
							DSGNode node= new DSGNode(writerTS);
							node.addEdge(commitTS);
							dsgGraph.put(writerTS, node);
						}
						//check outgoing rw (anti) dependency
						if(i < rwList.size()-1) {	//if this was the last version we wont have outgoing rw
							Pair<Long, LinkedList<Long>> nextVersionInfo = rwList.get(i+1);
							long nextWriterTS = nextVersionInfo.getFirst();
							//add the dependency to graph: commitTS->nextWriterTS
							if(dsgGraph.containsKey(commitTS)){
								DSGNode node = dsgGraph.get(commitTS);
								if (!node.getOutEdges().contains(nextWriterTS)){
									node.addEdge(nextWriterTS);
									dsgGraph.put(commitTS, node);
								}
							}else {
								DSGNode node= new DSGNode(commitTS);
								node.addEdge(nextWriterTS);
								dsgGraph.put(commitTS, node);
							}
						}
						break;
					}
				}
			} //else : won't happen, if txn has read an item it must be written earlier so entry should be present 
		}
		for(byte[] w: writeSet){
			if(singleTableMap.containsKey(w)){
				//since this txn has gone through ww conflict checking there should not be 
				//any newer version i.e. with timestamp > startTS. So just take the last version
				LinkedList< Pair< Long, LinkedList<Long>>> rwList = singleTableMap.get(w);
				Pair<Long, LinkedList<Long>> lastVersionInfo = rwList.getLast();
				for(long readerTS: lastVersionInfo.getSecond()){
					//this rw incoming dependency readerTS->commitTS
					if(dsgGraph.containsKey(readerTS)){
						DSGNode node = dsgGraph.get(readerTS);
						if (!node.getOutEdges().contains(readerTS)){
							node.addEdge(commitTS);
							dsgGraph.put(readerTS, node);
						}
					}else {
						DSGNode node= new DSGNode(readerTS);
						node.addEdge(commitTS);
						dsgGraph.put(readerTS, node);
					}
				}
				long prevWriterTS = lastVersionInfo.getFirst();
				//this is ww dependency prevWriterTS -> commitTS
				if(dsgGraph.containsKey(prevWriterTS)){
					DSGNode node = dsgGraph.get(prevWriterTS);
					if (!node.getOutEdges().contains(prevWriterTS)){
						node.addEdge(commitTS);
						dsgGraph.put(prevWriterTS, node);
					}
				}else {
					DSGNode node= new DSGNode(prevWriterTS);
					node.addEdge(commitTS);
					dsgGraph.put(prevWriterTS, node);
				}
				//add this version info 
				Pair<Long, LinkedList<Long>> versionInfo = new Pair<Long, LinkedList<Long>>();
				versionInfo.setFirst(commitTS);
				versionInfo.setSecond(new LinkedList<Long>());
				rwList.add(versionInfo);
				//update rwtable
				singleTableMap.put(w, rwList);
			} else {
				//add this version info 
				LinkedList< Pair< Long, LinkedList<Long>>> rwList = new LinkedList<Pair<Long,LinkedList<Long>>>();
				Pair<Long, LinkedList<Long>> versionInfo = new Pair<Long, LinkedList<Long>>();
				versionInfo.setFirst(commitTS);
				versionInfo.setSecond(new LinkedList<Long>());
				rwList.add(versionInfo);
				//update rwtable
				singleTableMap.put(w, rwList);
			}			
		}
		rwMap.put(tableName, singleTableMap);
	}
	
	public boolean checkCycle(long commitTS){				
		Stack<Long> nodeStack = new Stack<Long>();
		Vector<Long> visitedNodes = new Vector<Long>();
		if(!dsgGraph.containsKey(commitTS))
			return true;
		
		LinkedList<Long> outEdges = dsgGraph.get(commitTS).getOutEdges(); 
		for(long node: outEdges) {
			nodeStack.add(node);
		}	
		visitedNodes.add(commitTS);
		while(!nodeStack.isEmpty()){
			long ts = nodeStack.pop();			
			if(ts==commitTS) {	//found edge back to start node..cycle found
				//logger.logDebug("<DSGManager> Cycle found for transaction#"+startTid);
				//logger.logDebug("<DSGManager#T"+startTid+"> Number of nodes traversed:"+visitedNodes.size());
				return false;											
			} 
			if(visitedNodes.contains(ts))
				continue;			
			
			visitedNodes.add(ts);		
			if(ts!=commitTS) {
				if(dsgGraph.containsKey(ts)) {
					LinkedList<Long> edges = dsgGraph.get(ts).getOutEdges(); 
					for(long node: edges) {
						nodeStack.add(node);
					}
				}
			}
		}		
		return true;
	}
	
	private void removeNode(long commitTS){
		dsgGraph.remove(commitTS);
	}
	
	public void abort(long commitTS){
		removeNode(commitTS);
	}
	
}
