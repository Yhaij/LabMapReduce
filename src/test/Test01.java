package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test01 {
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://10.1.13.111:8020/user/mysql"), conf);
		InputStream in = fs.open(new Path("hdfs://10.1.13.111:8020/user/mysql/crawler_expert_unit_result/part-r-00000"));
		BufferedReader br = new  BufferedReader(new InputStreamReader(in, "utf-8"));
		String line;
		int num = 0;
		while((line = br.readLine())!=null){
//			String[] strs = line.split("'");
//			if(strs[7].equals("null")){
//				System.out.println(line);
//				num = num + 1;
//			}
//			if(strs.length != 11){
//				System.out.println(line);
//				num ++;
//			}
			System.out.println(line);
			if(line.split("'").length == 6)
				num++;
//			int flag = Integer.parseInt(strs[3]);
//			if (!(flag > 0 && flag <= 4)) {// 判断是关联表还是向量特征表
//				System.out.println(line);
//				num ++;
//			}
//			if(strs[0].equals("ae23702a-b7ad-11e6-af90-005056b3f30e")){
//				System.out.println(line);
//			}
		}
		System.out.println(num);
		br.close();
		in.close();
		fs.close();
	}
}
