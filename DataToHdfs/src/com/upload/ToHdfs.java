package com.upload;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ToHdfs {

	public static void main(String[] args) {
		while(true){
		String dir="/home/kexu/test/";
		File dirfile=new File("/home/kexu/test/");
		String[] filelist=dirfile.list();
		for(int i=0;i<filelist.length;i++)
		{
			if(!filelist[i].endsWith("lock")){
			File file=new File(dir+filelist[i]);
			
			upload("hdfs://kexu:9000/indata/",dir+filelist[i]);
			file.delete();
			}
		}
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
		}

	}
	
	public static void upload(String dst,String src) {
		String uri="hdfs://kexu:9000/";
		Configuration conf=new Configuration();
		FileSystem fs;	
			try {
				
				fs=FileSystem.get(URI.create(uri),conf);
				Path pathDst = new Path(dst);  
			    Path pathSrc = new Path(src);           
			    fs.copyFromLocalFile(pathSrc, pathDst); 
			  
			    fs.close();  
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	
		
	}


