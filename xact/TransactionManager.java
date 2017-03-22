package xact;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import util.*;

public class TransactionManager {
	
	public static final String STS_CTR_ROWID = "sts";	//stable ts counter	
	
	protected static TransactionManager tManager= null; 
	
	private DSGManagerInterface dsgManager;
	private HTable counterTable;
	
	private HTable dsgTable;
	private HTable cmtLogTable;
	//private HTable precmtTable;
	private HTable storageTable;
	private Logger logger;
	static Random rndTable ;
	static Random rndTID ;
	Batcher batcher;
	
	static{		
		rndTID = new Random(System.currentTimeMillis());
	}
	
	public TransactionManager(HTable dsgTable, HTable storageTable){
		this.dsgTable = dsgTable;
		this.storageTable = storageTable;
		tManager = this;
		batcher = Batcher.getBatcher();
	}
	
	public TransactionManager(HTable dsgTable){
		this.dsgTable = dsgTable;
		tManager = this;
		batcher = Batcher.getBatcher();
	}
	
	public TransactionManager(){
		Config config = Config.getInstance();
		try{			
			// String dsgTableName = config.getStringValue("dsgTableName");
			// String cmtLogTableName = config.getStringValue("cmtLogTableName");					
			// cmtLogTable = new HTable(config.getHBaseConfig(), cmtLogTableName);
			// dsgManager = new DSGManagerImpl(dsgTableName,cmtLogTable);
			//precmtTable = new HTable(config.getHBaseConfig(), "PRECMT");
		}catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
		logger = Config.getInstance().getLogger();
		tManager = this;
		batcher = Batcher.getBatcher();
	}
	
	public TransactionHandler startTransaction(){
		logger.logDebug("<TransactionManager> staring new transaction");
		try {
			String host = Config.getInstance().getStringValue("timestampServerName");
			int port = Config.getInstance().getIntValue("timestampServerPort");
			Socket socket = new Socket(host, port);
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeInt(1);
	        DataInputStream dis = new DataInputStream(socket.getInputStream());
	        long sts = dis.readLong();
	        int tid = dis.readInt();
	        dis.close();
	        dos.close();
	        socket.close();					
	        logger.logDebug("<TransactionManager> got tid:"+tid);
			logger.logDebug("<TransactionManager> got snapshot time:"+sts);
			return new TransactionHandler(this,tid,sts);
		}catch(IOException e){
			e.printStackTrace();
			logger.logDebug("<TransactionManager> Error in getting STS and TID from TimestampServer");
			return null;
		}
	}
	
	
	public long getCommitTimestamp() throws IOException{
		//increment current ts counter by 1		 
		//long ts = counterTable.incrementColumnValue(Bytes.toBytes(TS_CTR_ROWID), Bytes.toBytes("d"), Bytes.toBytes("val"), 1);	
		String host = Config.getInstance().getStringValue("timestampServerName");
		int port = Config.getInstance().getIntValue("timestampServerPort");
		Socket socket = new Socket(host, port);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(3);
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        long ts = dis.readLong();
        dis.close();
        dos.close();
        socket.close();
        logger.logDebug("<TransactionManager> got commit timestamp:"+ts);
		return ts; //return new value
	}	
	
	public void advanceStableTS(long newSTS) throws IOException{
		//Put p = new Put(Bytes.toBytes(STS_CTR_ROWID));
		//p.add(Bytes.toBytes("d"), Bytes.toBytes("val"),Bytes.toBytes(newSTS));
		//boolean done = counterTable.checkAndPut(Bytes.toBytes(STS_CTR_ROWID), Bytes.toBytes("d"), Bytes.toBytes("val"),
				//Bytes.toBytes(newSTS-1), p);		
		String host = Config.getInstance().getStringValue("timestampServerName");
		int port = Config.getInstance().getIntValue("timestampServerPort");
		Socket socket = new Socket(host, port);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(2);
		dos.writeLong(newSTS);
        dos.close();
        socket.close();
        
	}	
	
	
	
	public static TransactionManager getInstance(){
		return tManager;
	}

	public DSGManagerInterface getDSGManager() {
		return dsgManager;
	}
	
}
