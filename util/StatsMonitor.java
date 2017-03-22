package util;

import java.io.*;
import java.util.*;
import java.net.*;

public class StatsMonitor {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		class HostPort {
			String host;
			int port;
		}
		Vector<HostPort> hostList = new Vector<HostPort>();
		
		if (args[0].contains("-h=")) {
			String hostSpec = args[0].substring(3);
			StringTokenizer tok = new StringTokenizer(hostSpec, ":");
			String host = tok.nextToken();
			int port = Integer.parseInt(tok.nextToken());
			HostPort hp = new HostPort();
			hp.host = host;
			hp.port = port;
			hostList.add(hp);
		} else {
			String file = args[0];
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line="";	
		
			while ((line = reader.readLine()) != null) {
				if(line.startsWith("#"))
					continue;
				StringTokenizer tok = new StringTokenizer(line, ":");
				String host = tok.nextToken();
				int port = Integer.parseInt(tok.nextToken());
				HostPort hp = new HostPort();
				hp.host = host;
				hp.port = port;
				hostList.add(hp);
			}
		}
		
		if (args.length == 2) {
			if (args[1].equals("stop")) {
				for(HostPort hp : hostList) {
					try {
						System.out.println("Stopping "+hp.host+":"+hp.port);
						Socket socket = new Socket(hp.host, hp.port);
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						out.writeInt(1);
						socket.close();
					} catch(Exception e){}
				}
				return;
			}			
		}
		
		Calendar cal = Calendar.getInstance();		
		String month = cal.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);		
		String fileName = "report_"+month+cal.get(Calendar.DATE)+"_"+cal.getTimeInMillis()+".txt";
		String fileNameCV = "report_"+month+cal.get(Calendar.DATE)+"_"+cal.getTimeInMillis()+".csv";
		PrintWriter reporterCSV = new PrintWriter(fileNameCV);
		PrintWriter reporter = new PrintWriter(fileName);
		reporterCSV.println("Clients,Total Txns,Total Commits,Total Aborts,Avg CommitTime,Avg ExecTime");
		while(true) {
			
			int numTxn=0;
			int numCmt=0;
			int numAbort = 0;
			float sumCmt=0;
			float sumExec=0;
			int count=0;
			for(HostPort hp : hostList) {
				try {
				System.out.println("Getting reports from "+hp.host+":"+hp.port);
				Socket socket = new Socket(hp.host, hp.port);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				out.writeInt(0);
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				StatReport report = (StatReport) in.readObject();
				System.out.println("Total="+report.numTxn+" Cmt="+report.numCmt+" Abrt="+report.numAbort+" avgCmt="+report.avgCmt+" avgExec="+report.avgExec);
				
				if (report.numTxn < 50) {
					continue;
				}
				
				numTxn += report.numTxn;
				numCmt += report.numCmt;
				numAbort += report.numAbort;
				sumCmt += report.numTxn * report.avgCmt;
				sumExec += report.numTxn * report.avgExec;
				count++;
				}catch(Exception e) { System.out.println("Could not get reports from "+hp.host+":"+hp.port+" error:"+e.getMessage());}
			}
			float avgCmt = sumCmt / numTxn;
			float avgExec = sumExec / numTxn;
			System.out.println(new Date().toString());
			System.out.println("Num Clients:"+count);
			System.out.println("Total Txns:"+numTxn);
			System.out.println("Total Commits:"+numCmt);
			System.out.println("Total Aborts:"+numAbort);
			System.out.println("Avg time for commit:"+avgCmt);
			System.out.println("Avg time for exec:"+avgExec);
			System.out.println();
			String logLine = "Clients:"+count+" Txns:"+numTxn+" Cmts:"+numCmt+" Aborts:"+numAbort+" AvgCmt:"+avgCmt +" AvgExec:"+avgExec;
			String logLineCSV = count+","+numTxn+","+numCmt+","+numAbort+","+avgCmt +","+avgExec;
			reporter.println(logLine);
			reporterCSV.println(logLineCSV);
			reporter.flush();
			reporterCSV.flush();
			Thread.sleep(60*1000);
		}
		

	}

}
