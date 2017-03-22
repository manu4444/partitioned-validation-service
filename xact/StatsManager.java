package xact;

import java.util.*;

import util.*;

public class StatsManager {
	
	static StatContainer delayDSGStats = new StatContainer();
	static StatContainer delayVALStats = new StatContainer();
	static StatContainer delayCIStats= new StatContainer();
	static StatContainer delayCCStats= new StatContainer();
	static StatContainer delayExecStats= new StatContainer();
	static StatContainer delayCmtStats= new StatContainer();
	static StatContainer delayDepStats= new StatContainer();
	static StatContainer delayAdvSTSStats= new StatContainer();
	static StatContainer delayLockStats= new StatContainer();
	static StatContainer delayWaitStats= new StatContainer();
	
	static StatContainer delayCmtRdOnlyStats= new StatContainer();
	static StatContainer delayCmtUpdStats= new StatContainer();
	
	static StatContainer batchSizeStats = new StatContainer();
	static StatContainer batchDelayStats = new StatContainer();
	
	static int numSIAborts=0 , numSerAborts=0, numOtherAborts=0, numCommits=0, numTxn=0;
	static long startTime;
	
	public synchronized static void reportStats(long delayDSG, long delayVAL, long delayCI, long delayCC, long delayExec, long delayCmt,
			long delayDep, long delayAdvanceSTS, long delayLock, long delayWait, int abortReason, boolean readOnly){
		
		delayDSGStats.add(delayDSG);
		delayVALStats.add(delayVAL);
		delayCIStats.add(delayCI);
		delayCCStats.add(delayCC);
		delayExecStats.add(delayExec);
		delayCmtStats.add(delayCmt);
		delayDepStats.add(delayDep);
		delayAdvSTSStats.add(delayAdvanceSTS);
		delayLockStats.add(delayLock);
		delayWaitStats.add(delayWait);
		
		if(readOnly)
			delayCmtRdOnlyStats.add(delayCmt);
		else
			delayCmtUpdStats.add(delayCmt);
		
		if(abortReason == TransactionHandler.ABORT_SI)
			numSIAborts++;
		else if(abortReason == TransactionHandler.ABORT_SER)
			numSerAborts++;
		else if(abortReason == TransactionHandler.ABORT_OTHER)
			numOtherAborts++;
		else if(abortReason == TransactionHandler.NO_ABORT)
			numCommits++;
		
	}	
	public synchronized static void reportNewTxn(){
		numTxn++;
	}
	public synchronized static void reportStartTime(long start){
		startTime = start; 
	}
	
	public synchronized static void reportBatchStats(int size, long delay){
		batchSizeStats.add(size);
		batchDelayStats.add(delay);
	}
	
	public synchronized static void logStats() {
		Logger logger = new Logger();
		logger.logMsgOnly("********************Performance Statistics*********************");
		long runTime = System.currentTimeMillis()-startTime;
		logger.logMsgOnly("Start Time:"+new Date(startTime)+" End Time:"+new Date());
		logger.logMsgOnly("Total Run Time(sec):"+(runTime/1000));
		logger.logMsgOnly("");
		logger.logMsgOnly("Total Transactions:"+numTxn);
		logger.logMsgOnly("Number of Commits : "+numCommits);
		logger.logMsgOnly("Number of Aborts  : "+(numSIAborts+numSerAborts+numOtherAborts));
		logger.logMsgOnly("==>           SI  : "+numSIAborts);
		logger.logMsgOnly("==> Serialization : "+numSerAborts);
		logger.logMsgOnly("==>    By others  : "+numOtherAborts);
		logger.logMsgOnly("");
		logger.logMsgOnly("=======Performnance=======");
		logger.logMsgOnly("Execuation time   : AVG="+(long)delayExecStats.getMean()+" SD="+(long)delayExecStats.getStdDev()+ " MED="+(long)delayExecStats.getMedian() + " 90P="+(long)delayExecStats.getXthPercentile(90, StatContainer.ASC) +" MAX="+(long)delayExecStats.getMax());
		logger.logMsgOnly("Time to commit    : AVG="+(long)delayCmtStats.getMean()+" SD="+(long)delayCmtStats.getStdDev()+ " MED="+(long)delayCmtStats.getMedian() + " 90P="+(long)delayCmtStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayCmtStats.getMax());
		logger.logMsgOnly("");
		logger.logMsgOnly("Detailed:->");		
		logger.logMsgOnly("DSGUpdate stage   : AVG="+(long)delayDSGStats.getMean()+" SD="+(long)delayDSGStats.getStdDev()+ " MED="+(long)delayDSGStats.getMedian() + " 90P="+(long)delayDSGStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayDSGStats.getMax());
		logger.logMsgOnly("Validation stage  : AVG="+(long)delayVALStats.getMean()+" SD="+(long)delayVALStats.getStdDev()+ " MED="+(long)delayVALStats.getMedian() + " 90P="+(long)delayVALStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayVALStats.getMax());
		logger.logMsgOnly("Commit Incomplete : AVG="+(long)delayCIStats.getMean()+" SD="+(long)delayCIStats.getStdDev()+ " MED="+(long)delayCIStats.getMedian() + " 90P="+(long)delayCIStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayCIStats.getMax());
		logger.logMsgOnly("Commit Complete   : AVG="+(long)delayCCStats.getMean()+" SD="+(long)delayCCStats.getStdDev()+ " MED="+(long)delayCCStats.getMedian()  + " 90P="+(long)delayCCStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayCCStats.getMax()); 
		logger.logMsgOnly("");
		logger.logMsgOnly("Acquire Locks     : AVG="+(long)delayLockStats.getMean()+" SD="+(long)delayLockStats.getStdDev()+ " MED="+(long)delayLockStats.getMedian()  + " 90P="+(long)delayLockStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayLockStats.getMax());
		logger.logMsgOnly("Insert Dendencies : AVG="+(long)delayDepStats.getMean()+" SD="+(long)delayDepStats.getStdDev()+ " MED="+(long)delayDepStats.getMedian() + " 90P="+(long)delayDepStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayDepStats.getMax());
		logger.logMsgOnly("Advance STS       : AVG="+(long)delayAdvSTSStats.getMean()+" SD="+(long)delayAdvSTSStats.getStdDev()+ " MED="+(long)delayAdvSTSStats.getMedian() + " 90P="+(long)delayAdvSTSStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayAdvSTSStats.getMax());
		logger.logMsgOnly("Wait for status   : AVG="+(long)delayWaitStats.getMean()+" SD="+(long)delayWaitStats.getStdDev()+ " MED="+(long)delayWaitStats.getMedian()+ " 90P="+(long)delayWaitStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayWaitStats.getMax());
		
		logger.logMsgOnly("Commit Time ReadOnly   : AVG="+(long)delayCmtRdOnlyStats.getMean()+" SD="+(long)delayCmtRdOnlyStats.getStdDev()+ " MED="+(long)delayCmtRdOnlyStats.getMedian()+ " 90P="+(long)delayCmtRdOnlyStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayCmtRdOnlyStats.getMax());
		logger.logMsgOnly("Commit Time Update	  : AVG="+(long)delayCmtUpdStats.getMean()+" SD="+(long)delayCmtUpdStats.getStdDev()+ " MED="+(long)delayCmtUpdStats.getMedian()+ " 90P="+(long)delayCmtUpdStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)delayCmtUpdStats.getMax());
		
		logger.logMsgOnly("Batch Size        : AVG="+(long)batchSizeStats.getMean()+" SD="+(long)batchSizeStats.getStdDev()+ " MED="+(long)batchSizeStats.getMedian()+ " 90P="+(long)batchSizeStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)batchSizeStats.getMax());
		logger.logMsgOnly("Batch Delay       : AVG="+(long)batchDelayStats.getMean()+" SD="+(long)batchDelayStats.getStdDev()+ " MED="+(long)batchDelayStats.getMedian()+ " 90P="+(long)batchDelayStats.getXthPercentile(90,StatContainer.ASC) +" MAX="+(long)batchDelayStats.getMax());
		
		logger.flush();
		
		
	}
	
	public static StatReport getStatReport() {
		StatReport report = new StatReport();
		report.numTxn = numTxn;
		report.numCmt = numCommits;
		report.numAbort = numOtherAborts+numSIAborts+numSerAborts;
		report.avgCmt = delayCmtStats.getMean();
		report.avgExec = delayExecStats.getMean();
		return report;
	}
	
	public static void reset(){
		delayDSGStats = new StatContainer();
		delayVALStats = new StatContainer();
		delayCIStats= new StatContainer();
		delayCCStats= new StatContainer();
		delayExecStats= new StatContainer();
		delayCmtStats= new StatContainer();
		delayDepStats= new StatContainer();
		delayAdvSTSStats= new StatContainer();
		delayLockStats= new StatContainer();
		delayWaitStats= new StatContainer();
		
		delayCmtRdOnlyStats= new StatContainer();
		delayCmtUpdStats= new StatContainer();
		
		batchSizeStats = new StatContainer();
		batchDelayStats = new StatContainer();
		
		numSIAborts = numSerAborts = numOtherAborts = numCommits = numTxn = 0;
		startTime = System.currentTimeMillis();
	}
}
