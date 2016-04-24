package com.in;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import com.google.common.primitives.Bytes;
import com.kenai.jaffl.annotations.In;



public class ToHbase extends Thread {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		System.out.println("进入mr" +args.length);
	    Configuration conf = HBaseConfiguration.create();
	    conf.set(TableOutputFormat.OUTPUT_TABLE,"truedata");
	    Job job = new Job(conf, "data to hbase");
	    job.setJarByClass(ToHbase.class);	    
	    job.setMapperClass(myMapper.class);		   
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Put.class);
	    job.setInputFormatClass(TextInputFormat.class); 
	    job.setOutputFormatClass(MultiTableOutputFormat.class);
	    job.setNumReduceTasks(0);
	    TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
       // for(String p:args) //循环扫描数据存进HBase时解开此注释，在test.java中调用
     //   {
	  //     FileInputFormat.addInputPath(job,new Path(p));
     //  }
       FileInputFormat.addInputPath(job,new Path("hdfs://kexu:9000/indata/"));
       // FileInputFormat.addInputPath(job,new Path(args[0]));
	    job.submit();	    
	}	
	
	public static class myMapper extends Mapper<Object,Text,ImmutableBytesWritable, Put>
	{
		int index=1;
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException
		{
			
			String [] valuelist=value.toString().split(",");
			String [] iplist=valuelist[0].split("\\.");
			byte [] ip =new byte[4];			
			for(int i=0;i<iplist.length ;i++)
			{			
				ip[i]=(byte)(Integer.parseInt(iplist[i])& 0xFF);				
			}
			String protocol="";
			String pro="";
			if(valuelist[1].equals("HTTP"))
			{
				protocol="0";
				pro="http";
			}
			else
			{
				protocol="1";
				pro="https";
			}
			String url=valuelist[2].replace(pro+"://www.","");
			String [] urllist=url.split("\\/");
			System.out.println("url"+urllist.length);
			String time=valuelist[3];
			int patition=(int)(Math.random()*3);
			String index_="";
			if(index<10)
			{
				index_="00000"+Integer.toString(index);
			}
			else if(index<100)
			{
				index_="0000"+Integer.toString(index);
			}
			else if(index<1000)
			{
				index_="000"+Integer.toString(index);
			}
			else if(index<10000)
			{
				index_="00"+Integer.toString(index);
			}
			else if(index<100000)
			{
				index_="0"+Integer.toString(index);
			}
			else
			{
				index_=Integer.toString(index);
			}
			byte [] putkey=Bytes.concat(Integer.toString(patition).getBytes(),time.getBytes(),index_.getBytes());
			index++;
			Put put = new Put(putkey); 
			String urlend="";
			if(urllist.length!=1)
			{
				urlend=urllist[1];
			}
			put.add("urlfa".getBytes(),"url".getBytes(), 
			          urllist[0].getBytes());
			put.add("urlfa".getBytes(),"urlend".getBytes(), 
					urlend.getBytes());
			put.add("ipfa".getBytes(),"ip".getBytes(), 
			          ip);
			put.add("ipfa".getBytes(),"pro".getBytes(), 
			         protocol.getBytes());
			 if (!put.isEmpty())
	            {
	                ImmutableBytesWritable ib = new ImmutableBytesWritable();
	                ib.set("truedata".getBytes());
	                context.write(ib, put);
	            }      
			
			
		}
	}
	
	

}
