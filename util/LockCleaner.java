package util;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class LockCleaner {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if( args.length != 4 && args.length !=2 ) {
			System.out.println("Usage: <classname> <-clean|-info> -t=<tablename> [-s=<start-row> -e=<end-row>]");	
			System.exit(0);
		}
		
		String tableName = "";
		long start=0, end=0;
		boolean clean = true;
		for(int i=0; i<args.length; i++) {
			if (args[i].equals("-clean") || args[i].equals("-c")) {
				clean = true; 
				continue;
			} else if (args[i].equals("-info") || args[i].equals("-i")) {
				clean = false;
				continue;
			}
			String val =args[i].substring(args[i].indexOf('=')+1);
			if(args[i].startsWith("-t")) {
				tableName = val;
			} else if(args[i].startsWith("-s")) {
				start = Long.parseLong(val);
			} else {
				end = Long.parseLong(val);
			}
		}		
		Configuration config =  HBaseConfiguration.create();
		if(clean) {
			if (start==0 && end ==0)
				cleanLocks(config, tableName);
			else
				cleanLocks(config, tableName, start, end);
			
		}
		else {
			if (start==0 && end ==0)
				checkLocks(config, tableName);
			else
				checkLocks(config, tableName, start, end);
		}
	}

	public static void cleanLocks(Configuration config, String tableName, long start, long end) 
	throws IOException {
		System.out.println("Cleaning locks for table:"+tableName);
		HTable table = new HTable(config, tableName);
		table.setAutoFlush(false);
		for(long row=start; row <= end; row++) {
			Put p_lock = new Put(Bytes.toBytes(row));
			p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
			table.put(p_lock);
		}
		table.flushCommits();
		System.out.println("all locks cleared..");
	}
	
	public static void cleanLocks(Configuration config, String tableName) throws IOException {
		System.out.println("Cleaning locks for table:"+tableName);
		HTable table = new HTable(config, tableName);
		table.setAutoFlush(false);
		ResultScanner rs = table.getScanner(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME));
		Result r;
		while((r=rs.next()) != null) {	
			if (!r.isEmpty() && Bytes.toInt(r.value()) != -1) {
				Put p_lock = new Put(r.getRow());
				p_lock.add(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME),Bytes.toBytes(-1));
				table.put(p_lock);
			}
		}
		table.flushCommits();
		System.out.println("all locks cleared..");
	}
	
	public static void checkLocks(Configuration config, String tableName, long start, long end) throws IOException {
		System.out.println("Checking locks for table:"+tableName);
		HTable table = new HTable(config, tableName);
		for(long row=start; row <= end; row++) {
			Get g_lock = new Get(Bytes.toBytes(row));
			g_lock.addColumn(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME));
			Result res = table.get(g_lock);		
			if (!res.isEmpty() && Bytes.toInt(res.value()) != -1) {
				System.out.println("Row:"+row+" still locked by "+Bytes.toInt(res.value()));
			}
		}
		System.out.println("all rows checked..");
	}
	
	public static void checkLocks(Configuration config, String tableName) throws IOException {
		System.out.println("Checking locks for table:"+tableName);
		HTable table = new HTable(config, tableName);
		ResultScanner rs = table.getScanner(Bytes.toBytes(Constants.METADATA_FAMNAME),Bytes.toBytes(Constants.LOCK_COLNAME));
		Result r;
		while((r=rs.next()) != null) {			
			if (!r.isEmpty() && Bytes.toInt(r.value()) != -1) {
				System.out.println("Row:"+Bytes.toLong(r.getRow())+" still locked by "+Bytes.toInt(r.value()));
			}
		}
		System.out.println("all rows checked..");
	}
}
