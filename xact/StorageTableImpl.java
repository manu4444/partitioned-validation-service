package xact;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import util.Config;
import util.Constants;

public class StorageTableImpl implements StorageTableInterface {
	
	private HTable hTable;
	private String tableName;
	Vector<Put> writeSet;
	Vector<Get> readSet;
	Vector<Dependency> depList;	
	long commitTS, snapshotTS;
	int tid = -1;
	
	private int conflictTID = -1;	
	
	public StorageTableImpl(String tableName) throws IOException{
		this.tableName = tableName;
		hTable = new HTable(Config.getInstance().getHBaseConfig(),tableName);
		writeSet = new Vector<Put>();
		readSet = new Vector<Get>();
		depList = new Vector<Dependency>();		
	}
	
	public StorageTableImpl(String tableName, int tid, long snapshot_ts) throws IOException{
		this(tableName);
		this.tid  = tid;
		this.snapshotTS = snapshot_ts;
	}
	
	protected void finalize() {
		close();
	}
		
	
	@Override
	public int getWWConflict() throws IOException {		
		return conflictTID;
	}

	@Override
	public void commitWrites() throws IOException {
		Vector<Put> putList = new Vector<Put>();
		for(Iterator<Put> it = writeSet.iterator(); it.hasNext();) {
			Put p = it.next();
			//insert ts-> tid mapping in table and release lock
			Put pNew = new Put(p.getRow());
			pNew.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME),commitTS,Bytes.toBytes(tid));
			pNew.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
			putList.add(pNew);			
		}
		hTable.put(putList);
	}
	
	public void commitReadLocks() throws IOException {
		for(Iterator<Get> it = readSet.iterator(); it.hasNext();) {
			Get g = it.next();			
			Put pNew = new Put(g.getRow());
			pNew.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
			pNew.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READTS_COLNAME),Bytes.toBytes(commitTS));
						
			Get g1 = new Get(g.getRow());			
			g1.setMaxVersions(1);			
			g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READTS_COLNAME)); //get the readts			
			Result r = hTable.get(g1);
			long readts = Bytes.toLong(r.value());
			if(readts < commitTS)
				hTable.put(pNew);
		}
			
	}

	@Override
	public void deleteVersion(byte[] row, long ts) throws IOException {
		// TODO Auto-generated method stub

	}
	
	//commit a given data version (created by some other transaction)	
	//this method is used in recovery of some other committed transaction.
	@Override
	public void commitVersion(byte[] row, long ts) throws IOException {
		Put pNew = new Put(row);
		pNew.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME),ts,Bytes.toBytes(tid));
		pNew.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
		hTable.put(pNew);			
	}


	@Override
	public void flushWrites() throws IOException {
		// TODO Auto-generated method stub
		//no longer required in this version of StorageTableImpl
		//kept to be compatible with the interface definition
	}

	@Override
	public Result get(Get get) throws IOException {
		//read the latest version <= snapshot_ts, it will point to tid
		Get g =new Get(get.getRow());
		g.setTimeRange(0, snapshotTS+1);	//timerange => [min, max)
		g.setMaxVersions(1);	//we only want the latest 		
		g.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME));
		//g.addColumn(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(READSET_COLNAME));
		//System.out.println("locking row "+Bytes.toString(get.getRow()));
		//RowLock rl = hTable.lockRow(get.getRow());	//read and append your tid to readset, so get a lock
		//System.out.println("locked row "+Bytes.toString(get.getRow()));
		Result r = hTable.get(g);
		if(r.isEmpty()) {
			//hTable.unlockRow(rl);
			//System.out.println("unlocked row "+Bytes.toString(get.getRow()));
			Config.getInstance().getLogger().logWarning("<StorageTableImplV4#T"+tid+"> reading empty row");
			return r;
		}
		/* SI
		KeyValue kv = r.getColumnLatest(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(READSET_COLNAME));				
		String tid_string = String.valueOf(tid);
		String vector ="";
		
		if(kv!=null) {
			byte[] val = kv.getValue();
			vector = Bytes.toString(val); //existing readset				
			vector = vector.concat(","+tid_string);	//add tid to it
		}else
			vector = tid_string;
		//add tid in the read set of this version		
		Put p = new Put(get.getRow(),(r.getColumnLatest(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(TID_COLNAME))).getTimestamp());
		p.add(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(READSET_COLNAME), Bytes.toBytes(vector));		
		hTable.put(p);
		//hTable.unlockRow(rl);
		//System.out.println("unlocked row "+Bytes.toString(get.getRow()));
		 
		SI */
		
		//now read the actual data
		int readTid = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)));
		Config.getInstance().getLogger().logDebug("<StorageTableImplV4#T"+tid+"> reading with version-id:"+readTid);
		if(readTid < 0) {
			Config.getInstance().getLogger().logWarning("<StorageTableImplV4#T"+tid+"> null row");
			return null;
		}
		get.setTimeRange(0, readTid+1);
		get.setMaxVersions(1);
		Result rs = hTable.get(get);
		readSet.add(get);
		return rs;
	}

	@Override
	/*
	 * This method finds only rw (anti) dependencies
	 */
	public Vector<Dependency> getDependency() throws IOException {
		//get dependencies for objects I read
		for(Iterator<Get> it = readSet.iterator(); it.hasNext();) {
			Get g = it.next();
			/*
			//get incoming wr: writer of the version I read					
			
			Get gNew = new Get(g.getRow());
			gNew.setTimeRange(0,snapshotTS+1); // 0 to snapshot_ts
			gNew.setMaxVersions(1);
			gNew.addColumn(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(TID_COLNAME)); //tid of the writer	
			Result r= hTable.get(gNew);
			if(!r.isEmpty()){
				int fromTID = Bytes.toInt(r.value());
				if(fromTID!=tid && fromTID>0)
					depList.add(new Dependency(fromTID,tid));
			}			
			*/
			//get outgoing rw: writer of the immediately following version 
			Get g2 = new Get(g.getRow());
			g2.setTimeRange(snapshotTS+1,Long.MAX_VALUE);
			g2.setMaxVersions(1);
			g2.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME));
			g2.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME));
			//snapshot_ts to infinity, note that there should be only version after my snapshot_ts
			Result r= hTable.get(g2);
			if(!r.isEmpty()){
				if(r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)) && 
						Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME))) != -1) {
					int toTID = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)));
					if(toTID!=tid && toTID>0)
						depList.add(new Dependency(tid,toTID));	
				}
				else if(r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME))) {
					int toTID = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)));
					if(toTID!=tid && toTID>0)
						depList.add(new Dependency(tid,toTID));
				}				
			}
		}
		//get dependencies for object I wrote
		for(Iterator<Put> it = writeSet.iterator(); it.hasNext();) {
			Put p = it.next();
			Get g = new Get(p.getRow());
			g.setTimeRange(0, snapshotTS+1);
			g.setMaxVersions(1);
			//get incoming rw: reader(s) of the immediately preceding version
			g.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READSET_COLNAME));
			//get incoming ww: writer of the immediately preceding version
			//g.addColumn(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(TID_COLNAME)); 
			Result r= hTable.get(g);
			if(!r.isEmpty()) {				
				if(r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READSET_COLNAME))) {
					//tid of the reader
					String rsetVector = Bytes.toString(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READSET_COLNAME)));
					StringTokenizer stok = new StringTokenizer(rsetVector,",");
					while(stok.hasMoreElements()) {
						int tidR = Integer.parseInt(stok.nextToken());
						if(tidR!=tid && tidR >0)
							depList.add(new Dependency(tidR, tid));
					}
				}
				/*
				if(r.containsColumn(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(TID_COLNAME))) {
					//tid of the writer
					int tidW = Bytes.toInt(r.getValue(Bytes.toBytes(METADATA_FAMNAME),Bytes.toBytes(TID_COLNAME)));
					if(tidW!=tid && tidW>0)
						depList.add(new Dependency(tidW, tid));
				}
				*/
			}
		}
		return depList;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public Vector<Put> getWriteSet() {
		return writeSet;
	}
	
	public Vector<Get> getReadSet() {
		return readSet;
	}

	@Override
	public boolean lockWrites() throws IOException {
		//Config.getInstance().getLogger().logDebug("<StorageTableImplV4> acquiring locks..");
		for(Iterator<Put> it = writeSet.iterator(); it.hasNext();) {
			Put p = it.next();
			Put p_lock = new Put(p.getRow());
			p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(tid));
			//lock the lock column for this row if lock not already present			
			Get g1 = new Get(p.getRow());			
			g1.setMaxVersions(1);
			g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)); //get the latest version			
			g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)); //get the lock column
			g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READTS_COLNAME)); //get the readts
			boolean locked; //is the item already locked
			//System.out.println("acquiring rowlock on "+Bytes.toString(p.getRow()));
			//RowLock rl = hTable.lockRow(p.getRow()); //changed to optimize			
			Result r = hTable.get(g1);			
			boolean empty = false;
			int lockCol = -1;
			if(r.isEmpty()) {	// no result i.e. so its not already locked and no newer version 							
				locked = false;		
				empty = true;
			} else if(!r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)) ||
					(r.getColumnLatest(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME))).getTimestamp() <= snapshotTS)
				{
				//nobody has created a newer version, check the lock column
				lockCol = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME))); 
				if(lockCol == -1 || lockCol == tid) {  //not locked
					if(r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READTS_COLNAME))) {
						long readts = Bytes.toLong(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.READTS_COLNAME)));
						if(readts <= snapshotTS)
							locked = false;
						else {
							locked = true;
							conflictTID = 0;
						}
					}else 
						locked = false;				
				}
				else {//locked					
					locked = true;
					conflictTID = lockCol;
					Config.getInstance().getLogger().logDebug("<StorageTableImplV4#T"+tid+"> already locked "+
							tableName+":"+Bytes.toString(p.getRow()));
				}
				
			}else {	//there is at least one newer version, find the conflict id
				locked = true;
				Config.getInstance().getLogger().logDebug("<StorageTableImplV4#T"+tid+"> new version ("+
						(r.getColumnLatest(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME))).getTimestamp() +
						") present "+tableName+":"+Bytes.toString(p.getRow()));
				conflictTID = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)));
			}			
			
			if(!locked) { //not locked already, try to lock
				boolean done = true;
				if(!empty) 
					done = hTable.checkAndPut(p.getRow(), Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME), 
						Bytes.toBytes(-1),p_lock);
				else
					hTable.put(p_lock);
					
				if(!done) { //locking failed
					Get g = new Get(p.getRow());
					g.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME));
					r = hTable.get(g);
					conflictTID = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)));
					Config.getInstance().getLogger().logDebug("<StorageTableImplV4#T"+tid+"> can not acquire lock on "+
							tableName+":"+Bytes.toString(p.getRow()));
					return false;					
				}
			}else{ //already locked				
				return false;
			}
		}		
		return true;
	}

	//returns true if all locks are acquired successfully;	
	public boolean lockReads() throws IOException{
		for(Iterator<Get> it = readSet.iterator(); it.hasNext();) {	
			Get g = it.next();
			Put p_lock = new Put(g.getRow());
			p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(0));	
			Get g1 = new Get(g.getRow());			
			g1.setMaxVersions(1);
			g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)); //get the latest version			
			g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)); //get the lock column			
			//System.out.println("acquiring rowlock on "+Bytes.toString(p.getRow()));
			//RowLock rl = hTable.lockRow(p.getRow()); //changed to optimize			
			Result r = hTable.get(g1);			
			boolean empty = false;
			int lockCol = -1;
			if(r.isEmpty()) {	
				//should not happen
				return true;
			}
			else if(!r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)) ||
					(r.getColumnLatest(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME))).getTimestamp() <= snapshotTS)
			{
				//nobody has created a newer version, check the lock column
				lockCol = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME))); 
				if(lockCol == -1) {
					//not locked 
					boolean done = hTable.checkAndPut(g.getRow(), Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME), 
							Bytes.toBytes(-1),p_lock);
					if(!done){
						Get g2 = new Get(g.getRow());
						g2.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME));
						r = hTable.get(g2);
						int lockOwner = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)));
						if(lockOwner == 0 ){
							//its a read lock
							return true;
						}else
							return false;
					}
					
				}else if(lockCol == 0 || lockCol == tid){					
					//its either read locked or locked by this transaction only,
					//(locked by this transaction should not happen as we acquire read locks before write locks. 
					//so no need to acquire lock, so return true, 
					return true;
				}else {
					//its write locked
					return false;
				}
			}
			else{
				//a newer version is present, some transaction has already made a write to this object
				return false;
			}
		}
		return true;
	
	}

	@Override
	public void put(Put put) throws IOException{
		Put pNew = new Put(put.getRow(),tid);
		Iterator<Map.Entry<byte[], List<KeyValue>>> it1 =put.getFamilyMap().entrySet().iterator(); //map of list of KV pairs 
		while(it1.hasNext()){     //iterate through map
			for(Iterator<KeyValue> it2 = it1.next().getValue().iterator(); it2.hasNext();){ //iterate through list of KV pairs
				KeyValue kv = it2.next(); 
				pNew.add(kv.getFamily(), kv.getQualifier(), kv.getValue()); //add this kv data to new put					
			}
		}
		//Config.getInstance().getLogger().logDebug("<StorageTableImplV4> put data for "+Bytes.toString(pNew.getRow()));
		hTable.put(pNew);
		writeSet.add(put);
	}
	
	
	@Override
	public void put(List<Put> puts) throws IOException {
		List<Put> putList = new Vector<Put>();
		for (Put put : puts) {
			Put pNew = new Put(put.getRow(),tid);
			Iterator<Map.Entry<byte[], List<KeyValue>>> it1 =put.getFamilyMap().entrySet().iterator(); //map of list of KV pairs 
			while(it1.hasNext()){     //iterate through map
				for(Iterator<KeyValue> it2 = it1.next().getValue().iterator(); it2.hasNext();){ //iterate through list of KV pairs
					KeyValue kv = it2.next(); 
					pNew.add(kv.getFamily(), kv.getQualifier(), kv.getValue()); //add this kv data to new put					
				}
			}
			//Config.getInstance().getLogger().logDebug("<StorageTableImplV4> put data for "+Bytes.toString(pNew.getRow()));
			writeSet.add(put);
			putList.add(pNew);
		}
		
		hTable.put(putList);
		
	}
	
	private void releaseLock(byte[] row) throws IOException {
		Put p_lock = new Put(row);
		p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
		hTable.checkAndPut(row, Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME), 
		Bytes.toBytes(tid),p_lock);		
	}

	@Override
	public void rollbackWrites() throws IOException {
		List<Delete> deleteList = new Vector<Delete>();
		for(Iterator<Put> it = writeSet.iterator(); it.hasNext();) {
			//release lock				
			//releaseLock(p.getRow());
			Put p = it.next();
			Delete d = new Delete(p.getRow());			
			//delete all data columns
			Iterator<Map.Entry<byte[], List<KeyValue>>> it1 =p.getFamilyMap().entrySet().iterator(); //map of list of KV pairs 
			while(it1.hasNext()){     //iterate through map
				for(Iterator<KeyValue> it2 = it1.next().getValue().iterator(); it2.hasNext();){ //iterate through list of KV pairs
					KeyValue kv = it2.next(); 
					d.deleteColumn(kv.getFamily(), kv.getQualifier(), tid); 				
				}					
			}			
			deleteList.add(d);						
		}
		hTable.delete(deleteList);
	}

	@Override
	public void setCommitTS(long ts) {
		commitTS = ts;

	}

	@Override
	public void setSnapshotTS(long ts) {
		snapshotTS = ts;

	}

	@Override
	public void setTID(int tid) {
		this.tid = tid; 

	}
	
	@Override
	public void close() {
		try {
			hTable.close();
		} catch (IOException e){}	 	
	}

}
