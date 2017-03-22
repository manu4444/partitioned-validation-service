package util;

import java.io.*;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class RWSet implements Writable, Serializable {

	public long startTS;	
	public int tid;
	public List<Long> writeSet;	
	public List<Long> readSet;	
	
	public RWSet() {		
		startTS = tid = -1;
		writeSet = new Vector<Long>();
		readSet = new Vector<Long>();
	}
	
	public RWSet(long ts, int tid, List<Long> writeSet, List<Long> readSet){
		this.startTS = ts;		
		this.tid = tid;
		this.writeSet = writeSet;		
		this.readSet = readSet;		
	}
	
	public RWSet(long ts, int tid, List<Long> writeSet){
		this.startTS = ts;		
		this.tid = tid;
		this.writeSet = writeSet;		
		this.readSet = new Vector<Long>();	//empty readset		
	}
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		startTS = in.readLong();
		tid = in.readInt();
		int wsetSize = in.readInt();
		writeSet = new Vector<Long>();
		for(int i=0; i < wsetSize; i++){			
			writeSet.add(in.readLong());
		}
		int rsetSize = in.readInt();
		readSet = new Vector<Long>();
		for(int i=0; i < rsetSize; i++){			
			readSet.add(in.readLong());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(startTS);
		out.writeInt(tid);
		int wsetSize = writeSet.size();	
		out.writeInt(wsetSize);
		for(int i=0; i < wsetSize; i++) {
			out.writeLong(writeSet.get(i));
		}
		int rsetSize = readSet.size();
		out.writeInt(rsetSize);
		for(int i=0; i < rsetSize; i++){
			out.writeLong(readSet.get(i));			
		}
	}
	
	public static RWSet read(DataInput in) throws IOException {
		RWSet rwSet = new RWSet();
		rwSet.readFields(in);
		return rwSet;
	}	
}
