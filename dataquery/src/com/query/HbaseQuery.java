package com.query;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseQuery {

	static Configuration conf = null;

	static {

		conf = HBaseConfiguration.create();// hbase的配置信息

	}
	static String table = "truedata";

	public static void main(String[] args) throws IOException {

		//new HbaseQuery().getAll();
		//new HbaseQuery().getByTimeAndPro(2, 3, "https");
		//new HbaseQuery().getByTimeIpUrl(16,18,"http","google.com","1.1.1.1","20.251.250.250");
		new HbaseQuery().getAllByTimeUseCoprocessor(0,20)
	}

	/**
	 * 统计全部条数
	 * 
	 * @throws IOException
	 */
	public void getAll() throws IOException {
		HTable htable = new HTable(conf, table);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		long starttime = calendar.getTimeInMillis();
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		ResultScanner rs = htable.getScanner(scan);
		int num = 0;
		for (Result r : rs) {
			num++;
		
			

		}
		calendar.setTime(new Date());
		long endtime = calendar.getTimeInMillis();
		long duringtime = endtime - starttime;

		System.out.println("统计全部条数---   " + "条数:" + num + "   耗时:" + duringtime
				/ 1000.000);

	}

	/**
	 * 查询指定时间范围,指定协议类型的条数
	 * 
	 * @throws IOException
	 */

	public void getByTimeAndPro(int start, int end, String pro)
			throws IOException {
		String provalue = "";
		if (pro.equals("HTTP") || pro.equals("http")) {
			provalue = "0";
		} else if (pro.equals("HTTPS") || pro.equals("https")) {
			provalue = "1";
		}
		HTable htable = new HTable(conf, table);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		long starttime = calendar.getTimeInMillis();

		long starttimestamp = new Long("1450880826585") + start * 60 * 1000;
		long endtimestamp = new Long("1450880826585") + end * 60 * 1000;

		Scan scan1 = new Scan();
		String startkey1 = "0" + starttimestamp + "000001";
		String endkey1 = "0" + endtimestamp + "144000";
		scan1.setStartRow(startkey1.getBytes());
		scan1.setStopRow(endkey1.getBytes());
		Filter filter1 = new SingleColumnValueFilter("ipfa".getBytes(),
				"pro".getBytes(), CompareOp.EQUAL, provalue.getBytes());
		scan1.setFilter(filter1);
		ResultScanner rs1 = htable.getScanner(scan1);

		Scan scan2 = new Scan();
		String startkey2 = "0" + starttimestamp + "000001";
		String endkey2 = "0" + endtimestamp + "144000";
		scan2.setStartRow(startkey2.getBytes());
		scan2.setStopRow(endkey2.getBytes());
		Filter filter2 = new SingleColumnValueFilter("ipfa".getBytes(),
				"pro".getBytes(), CompareOp.EQUAL, provalue.getBytes());
		scan2.setFilter(filter2);
		ResultScanner rs2 = htable.getScanner(scan2);

		Scan scan3 = new Scan();
		String startkey3 = "0" + starttimestamp + "000001";
		String endkey3 = "0" + endtimestamp + "144000";
		scan3.setStartRow(startkey3.getBytes());
		scan3.setStopRow(endkey3.getBytes());
		Filter filter3 = new SingleColumnValueFilter("ipfa".getBytes(),
				"pro".getBytes(), CompareOp.EQUAL, provalue.getBytes());
		scan3.setFilter(filter3);
		ResultScanner rs3 = htable.getScanner(scan3);

		int num = 0;
		for (Result r : rs1) {
			/**
			 * for (Cell cell : r.rawCells()) { System. out .println(
			 * "Rowkey : " +Bytes. toString (r.getRow())+ "   Familiy:"+Bytes.
			 * toString (CellUtil.cloneFamily(cell))+"  Quilifier : " +Bytes.
			 * toString (CellUtil. cloneQualifier(cell))+ "   Value : " +Bytes.
			 * toString (CellUtil. cloneValue (cell))+ "   Time : "
			 * +cell.getTimestamp() ); }
			 **/

			num++;
		}

		for (Result r : rs2) {
			num++;
		}
		for (Result r : rs3) {
			num++;
		}
		calendar.setTime(new Date());
		long endtime = calendar.getTimeInMillis();
		long duringtime = endtime - starttime;

		System.out.println("查询指定时间范围协议类型的条数---   " + "条数:" + num + "   耗时:"
				+ duringtime / 1000.000);

	}

	public void getByTimeIpUrl(int start, int end, String pro,String url,String startip,String stopip) throws IOException {

		String provalue = "";
		if (pro.equals("HTTP") || pro.equals("http")) {
			provalue = "0";
		} else if (pro.equals("HTTPS") || pro.equals("https")) {
			provalue = "1";
		}
		byte [] startipCompare =new byte[4];
		String [] ip1=startip.split("\\.");
		for(int i=0;i<ip1.length ;i++)
		{			
			startipCompare[i]=(byte)(Integer.parseInt(ip1[i])& 0xFF);				
		}
		byte [] stopipCompare =new byte[4];
		String [] ip2=stopip.split("\\.");
		for(int i=0;i<ip2.length ;i++)
		{			
			stopipCompare[i]=(byte)(Integer.parseInt(ip2[i])& 0xFF);				
		}
		
		HTable htable = new HTable(conf, table);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		long starttime = calendar.getTimeInMillis();

		long starttimestamp = new Long("1450880826585") + start * 60 * 1000;
		long endtimestamp = new Long("1450880826585") + end * 60 * 1000;

		Scan scan1 = new Scan();
		
		String startkey1 = "0" + starttimestamp + "000001";
		String endkey1 = "0" + endtimestamp + "144000";
		scan1.setStartRow(startkey1.getBytes());
		scan1.setStopRow(endkey1.getBytes());
		
		Filter filter1_1 = new SingleColumnValueFilter("ipfa".getBytes(),
				"pro".getBytes(), CompareOp.EQUAL, provalue.getBytes());
		Filter filter1_2 = new SingleColumnValueFilter("urlfa".getBytes(),
				"url".getBytes(), CompareOp.EQUAL,url.getBytes());
		
		
		
		Filter filter1_3=new SingleColumnValueFilter("ipfa".getBytes(),"ip".getBytes(),CompareOp.LESS_OR_EQUAL,new BinaryComparator(stopipCompare));
		Filter filter1_4=new SingleColumnValueFilter("ipfa".getBytes(),"ip".getBytes(),CompareOp.GREATER_OR_EQUAL,new BinaryComparator(startipCompare));
		
		FilterList filterlist1=new FilterList(Operator.MUST_PASS_ALL);
		
		filterlist1.addFilter(filter1_1);
		filterlist1.addFilter(filter1_2);
		filterlist1.addFilter(filter1_3);
		filterlist1.addFilter(filter1_4);
		scan1.setFilter(filterlist1);
		
		
		ResultScanner rs1 = htable.getScanner(scan1);

		Scan scan2 = new Scan();
		String startkey2 = "0" + starttimestamp + "000001";
		String endkey2 = "0" + endtimestamp + "144000";
		scan2.setStartRow(startkey2.getBytes());
		scan2.setStopRow(endkey2.getBytes());
		Filter filter2_1 = new SingleColumnValueFilter("ipfa".getBytes(),
				"pro".getBytes(), CompareOp.EQUAL, provalue.getBytes());
		Filter filter2_2 = new SingleColumnValueFilter("urlfa".getBytes(),
				"url".getBytes(), CompareOp.EQUAL,url.getBytes());
		
		Filter filter2_3=new SingleColumnValueFilter("ipfa".getBytes(),"ip".getBytes(),CompareOp.LESS_OR_EQUAL,new BinaryComparator(stopipCompare));
		Filter filter2_4=new SingleColumnValueFilter("ipfa".getBytes(),"ip".getBytes(),CompareOp.GREATER_OR_EQUAL,new BinaryComparator(startipCompare));
		FilterList filterlist2=new FilterList(Operator.MUST_PASS_ALL);
		filterlist2.addFilter(filter2_1);
		filterlist2.addFilter(filter2_2);
		filterlist2.addFilter(filter2_3);
		filterlist2.addFilter(filter2_4);
		scan2.setFilter(filterlist2);
		ResultScanner rs2 = htable.getScanner(scan2);

		Scan scan3 = new Scan();
		String startkey3 = "0" + starttimestamp + "000001";
		String endkey3 = "0" + endtimestamp + "144000";
		scan3.setStartRow(startkey3.getBytes());
		scan3.setStopRow(endkey3.getBytes());
		Filter filter3_1 = new SingleColumnValueFilter("ipfa".getBytes(),
				"pro".getBytes(), CompareOp.EQUAL, provalue.getBytes());
		Filter filter3_2 = new SingleColumnValueFilter("urlfa".getBytes(),
				"url".getBytes(), CompareOp.EQUAL,url.getBytes());
		Filter filter3_3=new SingleColumnValueFilter("ipfa".getBytes(),"ip".getBytes(),CompareOp.LESS_OR_EQUAL,new BinaryComparator(stopipCompare));
		Filter filter3_4=new SingleColumnValueFilter("ipfa".getBytes(),"ip".getBytes(),CompareOp.GREATER_OR_EQUAL,new BinaryComparator(startipCompare));
		FilterList filterlist3=new FilterList(Operator.MUST_PASS_ALL);
		filterlist3.addFilter(filter3_1);
		filterlist3.addFilter(filter3_2);
		filterlist3.addFilter(filter3_3);
		filterlist3.addFilter(filter3_4);
		scan3.setFilter(filterlist3);
		ResultScanner rs3 = htable.getScanner(scan3);

		int num = 0;
		for (Result r : rs1) {
			
			 /** for (Cell cell : r.rawCells()) { System. out .println(
			 "Rowkey : " +Bytes. toString (r.getRow())+ "   Familiy:"+Bytes.
			 toString (CellUtil.cloneFamily(cell))+"  Quilifier : " +Bytes.
			  toString (CellUtil. cloneQualifier(cell))+ "   Value : " +Bytes.
			  toString (CellUtil. cloneValue (cell))+ "   Time : "
			 +cell.getTimestamp() ); }
			 */

			num++;
		}

		for (Result r : rs2) {
			
			/** for (Cell cell : r.rawCells()) { System. out .println(
					 "Rowkey : " +Bytes. toString (r.getRow())+ "   Familiy:"+Bytes.
					 toString (CellUtil.cloneFamily(cell))+"  Quilifier : " +Bytes.
					  toString (CellUtil. cloneQualifier(cell))+ "   Value : " +Bytes.
					  toString (CellUtil. cloneValue (cell))+ "   Time : "
					 +cell.getTimestamp() ); }
					 */
			num++;
		}
		for (Result r : rs3) {
			/** for (Cell cell : r.rawCells()) { System. out .println(
					 "Rowkey : " +Bytes. toString (r.getRow())+ "   Familiy:"+Bytes.
					 toString (CellUtil.cloneFamily(cell))+"  Quilifier : " +Bytes.
					  toString (CellUtil. cloneQualifier(cell))+ "   Value : " +Bytes.
					  toString (CellUtil. cloneValue (cell))+ "   Time : "
					 +cell.getTimestamp() ); }
					 */
			num++;
		}
		calendar.setTime(new Date());
		long endtime = calendar.getTimeInMillis();
		long duringtime = endtime - starttime;

		System.out.println("查询指定时间范围IP范围协议类型的条数---   " + "条数:" + num + "   耗时:"
				+ duringtime / 1000.000);
	}
 /**
 *
 *通过Coprocessor的方式查询指定时间内的条数
 */
	public void getAllByTimeUseCoprocessor(int start, int end) throws Throwable
	{
		
		long starttimestamp = new Long("1450880826585") + start * 60 * 1000;
		long endtimestamp = new Long("1450880826585") + end * 60 * 1000;
		
		HTable htable = new HTable(conf, table);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		long starttime = calendar.getTimeInMillis();
		
		final ServerCop.RowCountRequest request=ServerCop.RowCountRequest.newBuilder().setStarttime(starttimestamp).setEndtime(endtimestamp).build();
		Map<byte [] ,Long> re =htable.coprocessorService(ServerCop.CopCountService.class,null,null,new Batch.Call<ServerCop.CopCountService,Long>()
				{
			public Long call(ServerCop.CopCountService counter) throws IOException
			{
				ServerRpcController controller =new ServerRpcController();
				BlockingRpcCallback<ServerCop.RowCountResponse> rpccall=new BlockingRpcCallback<ServerCop.RowCountResponse>();
				counter.getCount(controller, request, rpccall);
				ServerCop.RowCountResponse resp=rpccall.get();
				long lc =0;
				if(null !=resp)
				{
					lc=resp.getRownum();
				}
				return lc;
			}
				});
		long r=0;
		
		for(Map.Entry<byte[], Long> entry:re.entrySet())
		{
			
			r+=entry.getValue();
		}
		calendar.setTime(new Date());
		long endtime = calendar.getTimeInMillis();
		long duringtime = endtime - starttime;
		System.out.println("利用Coprocessor查询指定时间范围的条数-----");
		System.out.println("条数:" + r + "   耗时:"
				+ duringtime / 1000.000 +"秒");
	}
}
