package com.bjtu.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {

	private FileSystem fs = null;
	
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://N5110:9000");
		fs = FileSystem.get(conf);
		//fs = FileSystem.get(new URI("hdfs://N5110:9000"), conf, "hadoop");
	}
	
	@Test
	public void upload() throws Exception {
		Path dst = new Path("hdfs://N5110:9000/upload/smarty-3.1.30.tar.gz");
		
		FSDataOutputStream os = fs.create(dst);
		
		FileInputStream is = new FileInputStream("/home/com0716/Downloads/smarty-3.1.30.tar.gz");
		
		IOUtils.copy(is, os);
	}
	
	@Test
	public void upload2() throws Exception, IOException {
		fs.copyFromLocalFile(new Path("/home/com0716/Downloads/smarty-3.1.30.tar.gz"), new Path("hdfs://N5110:9000/upload/"));
	}
	
	@Test
	public void download() throws IllegalArgumentException, IOException{
		fs.copyToLocalFile(new Path("hdfs://N5110:9000/upload/smarty-3.1.30.tar.gz"), new Path("/home/com0716/smarty-3.1.30.tar.gz"));
	}
	
	@Test
	public void listFile() throws Exception{
		 RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		 while (files.hasNext()){
			 LocatedFileStatus file = files.next();
			 Path filePath = file.getPath();
			 String filename = filePath.getName();
			 System.out.println(filename);
		 }
		 
		 System.out.println("--------------------------------------");
		 
		 FileStatus[] listStatus = fs.listStatus(new Path("/"));
		 for (FileStatus status : listStatus){
			 String name = status.getPath().getName();
			 System.out.println(name + " " + (status.isDirectory()?"is directory":"is file"));
		 }
	}
	
	@Test
	public void mkdir() throws IllegalArgumentException, IOException{
		fs.mkdirs(new Path("/test/hello/world"));
	}
	
	@Test
	public void rm() throws IllegalArgumentException, IOException{
		fs.delete(new Path("/test"), true);
	}	
}
