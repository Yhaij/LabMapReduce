package com.unit.clean;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UnitCleanMain {
//	public static List<String[]> areainfos = null;
//	public static List<String[]> cityinfos = null;
//	public static List<String[]> provinceinfos = null;
//	public static List<String[]> universityinfos = null;
//	public static List<String[]> institueinfos = null;
//
//	public static void init(Configuration conf){
//		areainfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/unit_areacode_map_new",conf);//从专利里提取出的单位信息
//		cityinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/unit_citycode_map",conf);//省，市，县及其代码信息
//		provinceinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/unit_provincecode_map",conf);//省份和省份代码信息
//		universityinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/china_universities",conf);//保存大学的信息
//		institueinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/china_institues",conf);//保存研究所的信息
//	}
	
	public static class Map extends
		Mapper<Object, Text, Text, Text>{
		int str_num = 6;            //字段总数
		int idPlace = 1;            //paper 1 patent 1   project 1
		int unitPlace = 5;          //paper 6 patent 8   project 8
		int totalUnitPlace = 5;     //paper 5 patent 9   project null use 8
//		int areaCodePlace = 22;     //paper 26 patent 23 project 22   关联67行，79行
		
		List<String[]> areainfos = null;
		List<String[]> cityinfos = null;
		List<String[]> provinceinfos = null;
		List<String[]> universityinfos = null;
		List<String[]> institueinfos = null;
		
		public List<String[]> getlist(String dirPath)throws IOException, InterruptedException{
			File file = new File(dirPath);
			List<String[]> list = new ArrayList<String[]>();
			for(String path: file.list()){
				if(path.startsWith("part-")){
					String filePath = dirPath + "/" + path;
					InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath),"utf-8");
					BufferedReader br = new BufferedReader(isr);
					String line;
					while((line = br.readLine()) != null){
						String[] strs = line.split("'");
						list.add(strs);
					}
					br.close();
					isr.close();
				}
				
			}
			return list;
		}
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			@SuppressWarnings("deprecation")
			Path[] p = context.getLocalCacheFiles();
			areainfos =  getlist("unit_areacode_map_new");
			cityinfos =  getlist("unit_citycode_map");
			provinceinfos =  getlist("unit_provincecode_map");
			universityinfos =  getlist("china_universities");
			institueinfos =  getlist("china_institues");
		}
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
				String[] strs = value.toString().replace("\t", "").split("'");
				if(strs.length>=str_num){
					String unit = strs[unitPlace]; //从字段中获取单位
					String firstOrganization = strs[totalUnitPlace]; //从字段中获取总的地址
					/***
					 * 得到需要匹配的最小单位
					 */
					if(!unit.equals("null")){
						unit = unit.split(";|,")[0];
					}else{
						unit = firstOrganization.replaceAll("\\(.*?\\)", "");    //去掉括号里的内容
						Pattern pattern = Pattern.compile("[^\\d\\s()（）].*$");  //去掉前缀是数字空格括号的
						Matcher m = pattern.matcher(firstOrganization);
						if(unit.equals("null")&&m.find()){  //m.find()必须要检验，否则会报错
							unit = m.group(0);
							unit = unit.split("!|;|/|,")[0];
						}
//				unit = pattern.split("");
					}
					strs[unitPlace] = unit; //写会要处理的单位
					System.out.println("----->Map:"+value.toString());
//					System.out.print("----->brfore:"+unit+strs[areaCodePlace]);
					context.write(new Text(fromUnitGetmap(strs)), new Text(""));
				} //formUnitGetmap 主要的单位处理函数
		}
		
		public String fromUnitGetmap(String[] strs){
			String unit = strs[unitPlace];//从字段中获取单位
			String id = strs[idPlace];//从字段中获取uuid
			String areacode = "null";//从字段中获取area_code
			CatalogUnitTableJoin infos = new CatalogUnitTableJoin(id); 
			if(!unit.equals("null")){
				isInUniversityOrInstituesTable(unit, infos);
				if(!infos.getTypeCode().equals("null")){
					return formatStr(strs, infos);//改在原文
//					return infos.toString();  //独立出另一张表
				}
				isUniversityStr(unit, areacode, infos);
				if(!infos.getTypeCode().equals("null")){
					return formatStr(strs, infos);
//					return infos.toString();
				}
				isCompanyStr(unit, areacode, infos);
				if(!infos.getTypeCode().equals("null")){
					return formatStr(strs, infos);
//					return infos.toString();
				}
				isInstituesStr(unit, areacode, infos);
				if(!infos.getTypeCode().equals("null")){
					return formatStr(strs, infos);
//					return infos.toString();
				}
				infos.setUnit(unit);
				infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
				infos.setTypeCode(Message.OTHERUNIT);
				infos.setRankCode(Message.OTHER);
				return formatStr(strs, infos);
//				return infos.toString();
			}else{
				infos.setTypeCode(Message.OTHERUNIT);
				infos.setRankCode(Message.OTHER);
				return formatStr(strs, infos);
//				return infos.toString();
			}
		}
		
		public String formatStr(String[] strs,CatalogUnitTableJoin infos){
			String result = strs[0]+"'"+strs[1]+"'"+strs[3]+"'"+strs[4]+"'"+infos.getUnit()+"'"+infos.getTypeCode()+"'"+infos.getProvinceCode();
			System.out.println("----->map out:"+ result);
			return result;
		}
		
		/**
		 * 判断unit是否在著名大学或研究院表中
		 * @param unit
		 */
		public void isInUniversityOrInstituesTable(String unit,CatalogUnitTableJoin infos){
			/*
			 * 首先判断是否在大学里
			 */
/*			for(String[] strs:universityinfos){
				for(String str:strs){
					System.out.print(str+" ");
				}
				System.out.println("");
			}*/
			for(String[] strs :universityinfos){
				if(unit.equals(strs[3])){
					infos.setUnit(unit);
					infos.setCity(strs[2]);
					infos.setProvinceCode(getCodefromUnit(strs[1]));
					infos.setTypeCode(Message.UNIVERSUTY);
					infos.setRankCode(Message.GOODUNIVERSITY);
					return;
				}
			}
			for(String[] strs :universityinfos){
				if(unit.startsWith(strs[3])){
					infos.setUnit(strs[3]);
					infos.setCity(strs[2]);
					infos.setProvinceCode(getCodefromUnit(strs[1]));
					infos.setTypeCode(Message.UNIVERSUTY);
					infos.setRankCode(Message.GOODUNIVERSITY);
					return;
				}
			}
			
			/*
			 *接着判断是否在研究所中 
			 */
			for(String[] strs :institueinfos){
				if(unit.equals(strs[3])){
					infos.setUnit(unit);
					infos.setCity(strs[2]);
					infos.setProvinceCode(getCodefromUnit(strs[1]));
					infos.setTypeCode(Message.INSTITUES);
					infos.setRankCode(Message.GOODINSTITUES);
					return;
				}
			}
			for(String[] strs :institueinfos){
				if(unit.startsWith(strs[3])){
					infos.setUnit(strs[3]);
					infos.setCity(strs[2]);
					infos.setProvinceCode(getCodefromUnit(strs[1]));
					infos.setTypeCode(Message.INSTITUES);
					infos.setRankCode(Message.GOODINSTITUES);
					return;
				}
			}
		}
		
		/**
		 * 字段匹配是否是大学
		 * @param unit
		 * @param areacode
		 * @param infos
		 */
		public void isUniversityStr(String unit,String areacode,CatalogUnitTableJoin infos){
			Pattern pattern = Pattern.compile(".*?(学校|学院|大学)");
			Matcher matcher = pattern.matcher(unit);
			if(matcher.find()){
				unit = matcher.group(0);
				if(!unit.contains("科学院")){
					infos.setUnit(unit);
					infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
					infos.setTypeCode(Message.UNIVERSUTY);
					infos.setRankCode(Message.COMMONUNIVERSITY);
				}
			}
		}
		
		/**
		 * 字段匹配是否为研究所
		 * @param unit
		 * @param areacode
		 * @param infos
		 */
		public void isInstituesStr(String unit,String areacode,CatalogUnitTableJoin infos){
			Pattern pattern = Pattern.compile(".*?(研究所|研究院|科学院|科院)");
			Matcher matcher = pattern.matcher(unit);
			if(matcher.find()){
				unit = matcher.group(0);
				infos.setUnit(unit);
				infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
				infos.setTypeCode(Message.INSTITUES);
				infos.setRankCode(Message.COMMONINSTITUES);
			}
		}
		
		/**
		 * 字段匹配是否为公司
		 * @param unit
		 * @param areacode
		 * @param infos
		 */
		public void isCompanyStr(String unit,String areacode,CatalogUnitTableJoin infos){
			unit = unit.replaceAll("\\(.*?\\)", ""); 
			Pattern pattern = Pattern.compile(".*?(公司|集团|医院|厂|场)");
			Matcher matcher = pattern.matcher(unit);
			if(matcher.find()){
				unit = matcher.group(0);
				infos.setUnit(unit);
				infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
				infos.setTypeCode(Message.COMPANY);
				infos.setRankCode(Message.COMMONCOMPANY);
			}
		}
		
		/**
		 * 从unit和areacode中获得中国省份的areacode
		 * @param unit
		 * @param areacode
		 * @return
		 */
		public String getProvinceFromUnit(String unit,String areacode){
			if(areacode.equals("null")){
				//在unit_areacode表中过滤一遍
				for(String[] strs: areainfos){
					if(unit.startsWith(strs[1])){
						areacode = strs[2];
//						System.out.println("----->>>>>>"+strs[1]);
						//判断是否是中国的省份
						return isProvinceCode(areacode);
					}
				}
				//areacode没有在unit_areacode中匹配到
				return getCodefromUnit(unit);
			}else{
				//判断是否为中国的省份
				return isProvinceCode(areacode);
			}
		}
		
		/**
		 * 根据areacode判断是否为在中国省份内
		 * @param areacode
		 * @return
		 */
		public String isProvinceCode(String areacode){
			for(String[] strs : provinceinfos){
				if(areacode.equals(strs[2])){
					return areacode;
				}
			}
			return "99";
		}
		
		/**
		 * 根据单位或省份的字段匹配来得到省份的code
		 * @param unit
		 * @return 从省份字段匹配出来的省份code
		 */
		public String getCodefromUnit(String province){
			for(String[] strs : cityinfos){
				if(province.contains(strs[1])){
					return strs[2];
				}
			}
			return "99";
		}

		
	} 
	
	public static class Reduce extends 
		Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("----->Reduce:"+key.toString());
			for(Text value: values){
				context.write(key, value);
			}

		}
	}
	
	
	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration(); // 启用默认配置
//		init(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		if (otherArgs.length != 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		conf.set("mapred.jar", "D:\\DataOperateJar\\UnitCleanMain.jar");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "unit clean");
		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysql/unit_areacode_map_new"));
		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysql/unit_citycode_map"));
		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysql/unit_provincecode_map"));
		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysql/china_universities"));
		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysql/china_institues"));
//		job.setJar("C:\\Users\\hp\\Desktop\\FirstHadoop.jar");
		job.setJarByClass(UnitCleanMain.class);
		job.setMapperClass(Map.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}

