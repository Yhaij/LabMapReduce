package com.paper.join.area;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 专家和文档的特征向量关联(根据其UUID进行关联)
 * @author yhj
 *
 */
public class PaperJournalExpertJoin {

	public static class Map extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] strs = value.toString().replace("\t", "").split("'");
			int flag = Integer.parseInt(strs[3]);
			if (flag > 0 && flag <= 4) {// 判断是关联表还是向量特征表
//				System.out.println("----->map:join table  " + value.toString());
				context.write(new Text(strs[4]), value);// 关联表将paper_id作为key
			} else {
//				System.out.print("----->before" + strs[6]);
				if(strs.length > 6){
					strs[6] = strs[6].split(",")[0];
				}else {
					strs[5] = strs[5] + "'null";
				}
//				System.out.println("----->after" + strs[6]);
				String result = StringUtils.join(strs, "'");
//				System.out.println("----->map:catalog table  " + result);
				context.write(new Text(strs[0]), new Text(result));// 向量特征表将paper_id作为key
			}
 		}
	}
	
	public static class  Reduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String> list = new ArrayList<String>();
			String result = null;
			for(int i = 0; i < 4;i++){
				list.add("null");
			}
			for(Text value:values){
				String[] strs = value.toString().split("'");
				int flag = Integer.parseInt(strs[3]);
				if (flag > 0 && flag <= 4) {
					list.set(flag - 1, strs[1]);
				} else {
					result = value.toString();
				}
			}
			if(result != null){
				result = result + "'" + StringUtils.join(list, "'");
				System.out.println("----->reduce:" + result);
				context.write(new Text(result), new Text(""));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // 启用默认配置
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		if (otherArgs.length != 3){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(3);
		}
		conf.set("mapred.jar","D:\\DataOperateJar\\PaperJournalExportUnit.jar");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "expert paper journal join");
		job.setJarByClass(PaperJournalExpertJoin.class);
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
