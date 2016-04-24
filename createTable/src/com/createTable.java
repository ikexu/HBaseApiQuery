package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class createTable {
	static Configuration conf=null;
	static{

	conf=HBaseConfiguration.create();//hbase的配置信息
	

	}
	public static void main(String[] args) throws Exception {
		
		createTable ct=new createTable();
		ct.createTable();
	}
	public void createTable()throws Exception{
		HBaseAdmin admin=new HBaseAdmin(conf);//客户端管理工具类
		
		if(admin.tableExists("truedata")){
		System.out.println("此表已经存在.......");
		}else{
		
		HTableDescriptor table=new HTableDescriptor("truedata");
		
		
		HColumnDescriptor col1=new HColumnDescriptor("ipfa");
		HColumnDescriptor col2=new HColumnDescriptor("urlfa");
		
		col1.setMaxVersions(100);
		table.addFamily(col1);
		col2.setMaxVersions(100);
		table.addFamily(col2);
		

		admin.createTable(table,"1".getBytes(),"2".getBytes(),3);
		admin.close();
		System.out.println("创建表成功!");
		}
		}
}
