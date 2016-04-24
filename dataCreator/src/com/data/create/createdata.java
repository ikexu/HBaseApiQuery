package com.data.create;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class createdata {
	private static int count = 144000;
	private static String[] protocols= {"HTTP","HTTPS"};
	private static String[] websites= {"www.baidu.com","www.163.com","www.google.com","www.sina.com"};

	public static void main(String[] args) throws IOException, InterruptedException {
		String path = args[0];
		//String path="/home/kexu/test/";
		while(true){	
			String filename = String.valueOf(System.currentTimeMillis());
			FileWriter fw = new FileWriter(path+filename+".lock");
			String result = getResult();
			fw.write(result);
			fw.close();
			File file = new File(path+filename+".lock");
			file.renameTo(new File(path+filename));
			Thread.sleep(60000);
		}
	}
	
	private static String getResult(){
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<count;i++){
			sb.append(getIP()+",");
			String protocol = getProtocol();
			sb.append(protocol+",");
			sb.append(getUrl(protocol)+",");
			sb.append(System.currentTimeMillis()+"\n");
		}
		return sb.toString();
		
	}
	
	private static String getIP(){
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf((int)(Math.random()*50)+1));
		for(int i=0;i<3;i++){
			sb.append("."+String.valueOf((int)(Math.random()*253)+1));
		}
		return sb.toString();
	}
	
	private static String getProtocol(){
		return protocols[Math.random()>0.5?1:0];
	}
	
	private static String getUrl(String protocol){
		String url = protocol+"://"+websites[(int)(Math.random()*3)]+"/"+getRandomString((int)(Math.random()*30));
		return url.toLowerCase();
	}
	
	private static String getRandomString(int length) {  
	    String base = "abcdefghijklmnopqrstuvwxyz0123456789";     
	    Random random = new Random();     
	    StringBuffer sb = new StringBuffer();     
	    for (int i = 0; i < length; i++) {     
	        int number = random.nextInt(base.length());     
	        sb.append(base.charAt(number));     
	    }     
	    return sb.toString();     
	 }     

}
