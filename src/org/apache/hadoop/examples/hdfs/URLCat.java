package org.apache.hadoop.examples.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class URLCat {

	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) throws MalformedURLException, IOException {
//		InputStream in=null;
//		try{
//			in = new URL("hdfs://localhost:9000/user/hadoop/test-in/test_file1.txt").openStream();
//			IOUtils.copyBytes(in, System.out, 4096, false);
//		}finally{
//			IOUtils.closeStream(in);
//		}
		Configuration conf = new Configuration();
		String uri="hdfs://localhost:9000/user/hadoop/test-in/test_file1.txt";
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try{
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096, false);
		}finally{
			IOUtils.closeStream(in);
		}
		
		String local="/home/hadoop/data/ta/raw.txt";
		String to="hdfs://localhost:9000/user/hadoop/raw.txt";
		InputStream newIn=new BufferedInputStream(new FileInputStream(local));
		fs = FileSystem.get(URI.create(to), conf);
		OutputStream out = fs.create(new Path(to), new Progressable() {
			
			@Override
			public void progress() {
				System.out.print(".");
			}
		});
		IOUtils.copyBytes(newIn, out, 4096, false);
	}
}
