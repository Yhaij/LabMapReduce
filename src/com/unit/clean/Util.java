package com.unit.clean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {
	public static List<String[]> getAllRecords(String path, Configuration conf){
//		Configuration conf = new Configuration();
		InputStream in = null;
		BufferedReader br = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(path),conf);
			FileStatus[] status = fs.listStatus(new Path(path));
			List<String[]> list = new ArrayList<>();
			for(FileStatus file : status){
				if (!file.getPath().getName().startsWith("part-")){
					continue;
				}
				in = fs.open(file.getPath());
				br = new BufferedReader(new InputStreamReader(in, "utf-8"));
				String line = null;
				while((line = br.readLine())!= null){
					String[] strs = line.split("'");
					list.add(strs);
				}
			}
			return list;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				in.close();
				br.close();
				fs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	
}
