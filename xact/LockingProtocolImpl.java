package xact;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import util.Constants;
import util.RWSet;

public class LockingProtocolImpl extends BaseEndpointCoprocessor implements
		LockingProtocol {

	@Override
	public List<LockCallResult> getSILocks(List<RWSet> rwSetList) throws IOException {
		List<LockCallResult> resultList = new Vector<LockCallResult>();
		HRegion region = ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();				
		HRegionInfo regionInfo = region.getRegionInfo(); 
		for (RWSet rwset : rwSetList ){
			List<Long> lockedItems = new Vector<Long>();
			boolean locked = true;
			for (long rowid : rwset.writeSet) {
				byte[] row = Bytes.toBytes(rowid);	
				if (HRegion.rowIsInRange(regionInfo, row)) {		// if this region holds this row, acquire lock
					if(getSILock(row, rwset.startTS, rwset.tid)) {					
						lockedItems.add(rowid);
					} else {
						// can not lock
						// release all acquired locks
						releaseWriteLocks(lockedItems);
						locked = false;
						break;	// no need to lock further
					}						
				}
			}
			LockCallResult result;
			if (locked) 
				result = new LockCallResult(rwset.tid, true, lockedItems);
			else
				result = new LockCallResult(rwset.tid, false);
			
			resultList.add(result);
		}
		System.out.println("On server");
		return resultList;
	}

	@Override
	public List<LockCallResult> getRWLocks(List<RWSet> rwSetList) throws IOException {		
		return null;

	}

	// returns true if it can acquire lock otherwise false
	private boolean getSILock(byte[] row, long startTS, int tid) throws IOException {	
		System.out.println("Getting lock for "+row);
		HRegion region = ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();
		Put p_lock = new Put(row);
		p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(tid));
		//lock the lock column for this row if lock not already present			
		Get g1 = new Get(row);			
		g1.setMaxVersions(1);
		g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)); //get the latest version			
		g1.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME)); //get the lock column		
		boolean locked; //is the item already locked		
		Integer lockId = region.obtainRowLock(row); 	
		try {
			Result r = region.get(g1, lockId);		
			int lockCol = -1;
			if(r.isEmpty()) {	// no result i.e. so its not already locked and no newer version 		
				//System.out.println("row not found for "+row);
				locked = false;			
			} else if(!r.containsColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME)) ||
				(r.getColumnLatest(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.TID_COLNAME))).getTimestamp() <= startTS)
			{
				//nobody has created a newer version, check the lock column
				lockCol = Bytes.toInt(r.getValue(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME))); 
				if(lockCol == -1 || lockCol == tid) {  //not locked
					locked = false;				
				}
				else { //locked		
					System.out.println("Already locked by "+lockCol);
					locked = true;				
				}			
			} else {	//there is at least one newer version
				System.out.println("newer version present "+row);
				locked = true;			
			}			
		
			if(!locked) { //not locked already, lock it				
				region.put(p_lock, lockId);			
				region.releaseRowLock(lockId);
				return true;			
			}else{ //already locked	
				region.releaseRowLock(lockId);
				return false;
			}
		} catch(IOException e){
			region.releaseRowLock(lockId);
			return false;
		}
	}
	
	@Override
	public void releaseWriteLocks(List<Long> rowList) throws IOException {
		HRegion region = ((RegionCoprocessorEnvironment)getEnvironment()).getRegion();				
		HRegionInfo regionInfo = region.getRegionInfo(); 
		
		Vector<Put> putList = new Vector<Put>();
		for (Long rowid: rowList) {
			byte[] row = Bytes.toBytes(rowid);
			if (HRegion.rowIsInRange(regionInfo, row)) {	
				Put p_lock = new Put(row);
				p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
				putList.add(p_lock);
			}
		}	
		
		if (putList.size() >0) {
			Put[] puts = new Put[putList.size()];
			puts = putList.toArray(puts);
			region.put(puts);
		}
	}
}
