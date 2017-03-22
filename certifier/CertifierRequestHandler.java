package certifier;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import xact.*;
import util.*;

public class CertifierRequestHandler extends Thread {

	Socket clientSocket;
	DependencyManager dsgManager;
	static int handlerCount = 0;
	int handlerID = 0;
	
	//statistics
	static long totalCmt =0 , totalAbort =0, totalSIAbort =0, totalSerAbort=0;
	
	public CertifierRequestHandler(Socket clientSocket, DependencyManager dsgManager) {
		this.clientSocket = clientSocket;
		this.dsgManager = dsgManager;
		handlerID = ++handlerCount;		
	}
	
	public void run() {
		try{
			Logger logger = Config.getInstance().getLogger();
			ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
			CertifierRequest request = (CertifierRequest) in.readObject();
			logger.logInfo("<Certifier> Handler#"+handlerID+ "Received certification request:"+request.toString());
			int abortStatus = TransactionHandler.NO_ABORT;
			if(request.getActionType() == CertifierRequest.CERTIFY_SERIALIZABLE) {
				abortStatus = checkSerializable(request);
			}
			else if(request.getActionType() == CertifierRequest.CERTIFY_BOTH) {
				abortStatus = checkBoth(request);
			}else if(request.getActionType() == CertifierRequest.CERTIFY_SI) {
				abortStatus = checkSI(request);
			}
			
			logTxn(request);
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
			out.writeInt(abortStatus);
			logger.logInfo("<Certifier> Handler#"+handlerID+" Sending response:"+abortStatus);
			out.close();
			in.close();
			clientSocket.close();
		}catch(Exception e) {
			e.printStackTrace();
		}finally{
			try{
				clientSocket.close();
			}catch(IOException ioe){}
		}
		
	}
	
	private void logTxn(CertifierRequest request){
		try{
			HTable committingTable = new HTable(Config.getInstance().getHBaseConfig(), "COMMITTING");	
			Put p = new Put(Bytes.toBytes(request.getTid()));
			p.add(Bytes.toBytes("d"),Bytes.toBytes(DSGManagerInterface.STATUS_COLNAME),
					Bytes.toBytes(TransactionHandler.COMMIT_INCOMPLETE));
			committingTable.put(p);
		}catch(IOException e){
			Config.getInstance().getLogger().logWarning("<Certifier> Handler#"+handlerID+" could not log transaction");
		}
	}
	
	private int checkBoth(CertifierRequest request) {
		Logger logger = Config.getInstance().getLogger();	
		long startTS = request.getStartTS();
		long commitTS = request.getCommitTS();
		//System.out.println("Received request");
		synchronized (dsgManager) {			
			boolean conflict = dsgManager.checkWWConflict(request.getRwSet(), startTS);
			if(!conflict) {
				logger.logDebug("<Certifier> Transaction#"+commitTS+" no ww conflict");

				dsgManager.addDependencies(request.getRwSet(), startTS, commitTS);
				
				boolean success = dsgManager.checkCycle(commitTS);
				if(!success){
					totalSerAbort++;
					totalAbort++;
					logger.logDebug("<Certifier> Transaction#"+commitTS+" found serialization conflict");
					dsgManager.abort(commitTS);
					return TransactionHandler.ABORT_SER;
				}
				else{
					totalCmt++;
					return TransactionHandler.NO_ABORT;
				}
			}else{
				totalSIAbort++;
				totalAbort++;
				return TransactionHandler.ABORT_SI;
			}
		}
		
	}
	
	private int checkSI(CertifierRequest request) {
		Logger logger = Config.getInstance().getLogger();	
		long startTS = request.getStartTS();
		long commitTS = request.getCommitTS();
		synchronized (dsgManager) {			
			boolean conflict = dsgManager.checkWWConflict(request.getRwSet(), startTS);
			
			if(!conflict) {
				logger.logDebug("<Certifier> Transaction#"+commitTS+" no ww conflict");
				dsgManager.addWrites(request.getRwSet(), startTS, commitTS);
				totalCmt++;
				return TransactionHandler.NO_ABORT;
			}else{
				totalSIAbort++;
				totalAbort++;
				return TransactionHandler.ABORT_SI;			
			}
		}
	}
	
	private int checkSerializable(CertifierRequest request) {		
		long startTS = request.getStartTS();
		long commitTS = request.getCommitTS();		
		synchronized (dsgManager) {			
			dsgManager.addDependencies(request.getRwSet(), startTS, commitTS);
			boolean commitStatus = dsgManager.checkCycle(commitTS);
			if(commitStatus == false){
				dsgManager.abort(commitTS);
				return TransactionHandler.ABORT_SER;
			}else{
				return TransactionHandler.NO_ABORT;
			}
		}		
		
		
	}
}
