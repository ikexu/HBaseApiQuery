package com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.ServerCop;
import com.ServerCop.RowCountRequest;
import com.ServerCop.RowCountResponse;
import com.google.common.primitives.Bytes;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;



public class MyCoprocessor extends ServerCop.CopCountService implements Coprocessor,CoprocessorService  {

	private RegionCoprocessorEnvironment environment=null;
	
	
	@Override
	public Service getService() {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
		if (env instanceof RegionCoprocessorEnvironment)
		{
			this.environment=(RegionCoprocessorEnvironment)env;
		}
		else
		{
			throw new CoprocessorException("Must be loaded on a table region");
		}
	
		
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getCount(RpcController controller, ServerCop.RowCountRequest request,
			RpcCallback<ServerCop.RowCountResponse> done) throws IOException {
		// TODO Auto-generated method stub
		
		HRegion rg=environment.getRegion();
		byte [] startkey=rg.getStartKey();
		byte [] endkey=rg.getEndKey();
		//byte [] RegionIndex=rg.getRegionName();
		byte [] RegionIndex=startkey;
		
		if(startkey==null||startkey.length==0)
		{
			RegionIndex="0".getBytes();
		}
		
		
		
	
		
		
		long starttime=request.getStarttime();
		long endtime=request.getEndtime();
		byte[] keystart=createStartRowKey(RegionIndex,starttime);
		byte[] keyend=createEndRowKey(RegionIndex,endtime);
		
		Scan sc=new Scan();
		sc.setStartRow(keystart);
		sc.setStopRow(keyend);
		RegionScanner scanner=rg.getScanner(sc);
		  List<Cell> results = new ArrayList<Cell>();  
	      boolean hasMore = false;  
	      byte[] lastRow = null;  
	      long count = 0;  
	      do {  
	        hasMore = scanner.next(results);  
	        for (Cell kv : results) {  
	          byte[] currentRow = CellUtil.cloneRow(kv);  
	          if (lastRow == null || !Arrays.equals(lastRow, currentRow)) {  
	            lastRow = currentRow;  	           
	            count=count+1;  
	          }  
	        }  
	        results.clear();  
	      } while (hasMore);       
	    
		
		ServerCop.RowCountResponse resp =ServerCop.RowCountResponse.newBuilder().setRownum(count).build();
		done.run(resp);
	}
	
	public static byte[] createStartRowKey(byte [] regionindex,long starttime)
	{
		byte [] k=Bytes.concat(regionindex,Long.toString(starttime).getBytes(),"000001".getBytes());
		return k;
				
	}
	public static byte[] createEndRowKey(byte [] regionindex,long endtime)
	{
		byte [] k=Bytes.concat(regionindex,Long.toString(endtime).getBytes(),"144000".getBytes());
		return k;
				
	}
	
	

	
	
	

}
