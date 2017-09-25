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
//		areainfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/unit_areacode_map_new",conf);//��ר������ȡ���ĵ�λ��Ϣ
//		cityinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/unit_citycode_map",conf);//ʡ���У��ؼ��������Ϣ
//		provinceinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/unit_provincecode_map",conf);//ʡ�ݺ�ʡ�ݴ�����Ϣ
//		universityinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/china_universities",conf);//�����ѧ����Ϣ
//		institueinfos = Util.getAllRecords("hdfs://10.1.13.111:8020/user/mysql/china_institues",conf);//�����о�������Ϣ
//	}
	
	public static class Map extends
		Mapper<Object, Text, Text, Text>{
		int str_num = 6;            //�ֶ�����
		int idPlace = 1;            //paper 1 patent 1   project 1
		int unitPlace = 5;          //paper 6 patent 8   project 8
		int totalUnitPlace = 5;     //paper 5 patent 9   project null use 8
//		int areaCodePlace = 22;     //paper 26 patent 23 project 22   ����67�У�79��
		
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
					String unit = strs[unitPlace]; //���ֶ��л�ȡ��λ
					String firstOrganization = strs[totalUnitPlace]; //���ֶ��л�ȡ�ܵĵ�ַ
					/***
					 * �õ���Ҫƥ�����С��λ
					 */
					if(!unit.equals("null")){
						unit = unit.split(";|,")[0];
					}else{
						unit = firstOrganization.replaceAll("\\(.*?\\)", "");    //ȥ�������������
						Pattern pattern = Pattern.compile("[^\\d\\s()����].*$");  //ȥ��ǰ׺�����ֿո����ŵ�
						Matcher m = pattern.matcher(firstOrganization);
						if(unit.equals("null")&&m.find()){  //m.find()����Ҫ���飬����ᱨ��
							unit = m.group(0);
							unit = unit.split("!|;|/|,")[0];
						}
//				unit = pattern.split("");
					}
					strs[unitPlace] = unit; //д��Ҫ����ĵ�λ
					System.out.println("----->Map:"+value.toString());
//					System.out.print("----->brfore:"+unit+strs[areaCodePlace]);
					context.write(new Text(fromUnitGetmap(strs)), new Text(""));
				} //formUnitGetmap ��Ҫ�ĵ�λ������
		}
		
		public String fromUnitGetmap(String[] strs){
			String unit = strs[unitPlace];//���ֶ��л�ȡ��λ
			String id = strs[idPlace];//���ֶ��л�ȡuuid
			String areacode = "null";//���ֶ��л�ȡarea_code
			CatalogUnitTableJoin infos = new CatalogUnitTableJoin(id); 
			if(!unit.equals("null")){
				isInUniversityOrInstituesTable(unit, infos);
				if(!infos.getTypeCode().equals("null")){
					return formatStr(strs, infos);//����ԭ��
//					return infos.toString();  //��������һ�ű�
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
		 * �ж�unit�Ƿ���������ѧ���о�Ժ����
		 * @param unit
		 */
		public void isInUniversityOrInstituesTable(String unit,CatalogUnitTableJoin infos){
			/*
			 * �����ж��Ƿ��ڴ�ѧ��
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
			 *�����ж��Ƿ����о����� 
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
		 * �ֶ�ƥ���Ƿ��Ǵ�ѧ
		 * @param unit
		 * @param areacode
		 * @param infos
		 */
		public void isUniversityStr(String unit,String areacode,CatalogUnitTableJoin infos){
			Pattern pattern = Pattern.compile(".*?(ѧУ|ѧԺ|��ѧ)");
			Matcher matcher = pattern.matcher(unit);
			if(matcher.find()){
				unit = matcher.group(0);
				if(!unit.contains("��ѧԺ")){
					infos.setUnit(unit);
					infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
					infos.setTypeCode(Message.UNIVERSUTY);
					infos.setRankCode(Message.COMMONUNIVERSITY);
				}
			}
		}
		
		/**
		 * �ֶ�ƥ���Ƿ�Ϊ�о���
		 * @param unit
		 * @param areacode
		 * @param infos
		 */
		public void isInstituesStr(String unit,String areacode,CatalogUnitTableJoin infos){
			Pattern pattern = Pattern.compile(".*?(�о���|�о�Ժ|��ѧԺ|��Ժ)");
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
		 * �ֶ�ƥ���Ƿ�Ϊ��˾
		 * @param unit
		 * @param areacode
		 * @param infos
		 */
		public void isCompanyStr(String unit,String areacode,CatalogUnitTableJoin infos){
			unit = unit.replaceAll("\\(.*?\\)", ""); 
			Pattern pattern = Pattern.compile(".*?(��˾|����|ҽԺ|��|��)");
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
		 * ��unit��areacode�л���й�ʡ�ݵ�areacode
		 * @param unit
		 * @param areacode
		 * @return
		 */
		public String getProvinceFromUnit(String unit,String areacode){
			if(areacode.equals("null")){
				//��unit_areacode���й���һ��
				for(String[] strs: areainfos){
					if(unit.startsWith(strs[1])){
						areacode = strs[2];
//						System.out.println("----->>>>>>"+strs[1]);
						//�ж��Ƿ����й���ʡ��
						return isProvinceCode(areacode);
					}
				}
				//areacodeû����unit_areacode��ƥ�䵽
				return getCodefromUnit(unit);
			}else{
				//�ж��Ƿ�Ϊ�й���ʡ��
				return isProvinceCode(areacode);
			}
		}
		
		/**
		 * ����areacode�ж��Ƿ�Ϊ���й�ʡ����
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
		 * ���ݵ�λ��ʡ�ݵ��ֶ�ƥ�����õ�ʡ�ݵ�code
		 * @param unit
		 * @return ��ʡ���ֶ�ƥ�������ʡ��code
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
		Configuration conf = new Configuration(); // ����Ĭ������
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

