package com.in;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.genetics.BinaryChromosome;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.compiler.ir.operands.Array;

public class test { //用以实时扫描数据存进Hase

	private static final String uri = "hdfs://kexu:9000/";
	private static Configuration conf = new Configuration();
	static ArrayList <Path> avaiFiles=new ArrayList<Path>();
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		while (true) {
			
			String[] arg = getfilelist();
			if (arg.length != 0) {
				ToHbase.main(arg);
				for (String p : arg) {
					fs.delete(new Path(p), false);
					System.out.println("删除...." + p.toString());
				}
			}
			Thread.sleep(10000);**/
			 
		
	}

	
	public static String[] getfilelist() throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		RemoteIterator<LocatedFileStatus> fl = fs.listFiles(new Path(
				"/indata/"), false);
		List<Path> filelist = new ArrayList<Path>();
		while (fl.hasNext()) {
			filelist.add(fl.next().getPath());
		}
		String[] arg = new String[filelist.size()];
		for (int i = 0; i < filelist.size(); i++) {
			arg[i] = filelist.get(i).toString();
			System.out.println("加载...." + filelist.get(i).toString());
		}
		return arg;
	}
	
}
