package xact;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import java.util.*;

public class LockCallResult implements Writable {
	int tid;
	boolean allLocked;
	List<Long> lockedItems;
	
	public LockCallResult(int tid, boolean locked, List<Long> lockedItems){
		this.tid = tid;
		this.allLocked = locked;
		this.lockedItems = lockedItems;
	}
	
	public LockCallResult(int tid, boolean locked){
		this.tid = tid;
		this.allLocked = locked;
		this.lockedItems = new Vector<Long>();
	}
	
	public LockCallResult() {
		tid = -1;
		allLocked = false;
		lockedItems = new Vector<Long>();
	}
	@Override
	public void readFields(DataInput in) throws IOException {		
		tid = in.readInt();
		allLocked = in.readBoolean();
		int size = in.readInt();
		lockedItems = new Vector<Long>();
		for(int i=0; i<size; i++) {
			Long item = in.readLong();
			lockedItems.add(item);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(tid);
		out.writeBoolean(allLocked);		
		int size = lockedItems.size();
		out.writeInt(size);
		for(int i=0; i<size; i++) {
			Long item = lockedItems.get(i);
			out.writeLong(item);
		}
	}

}
