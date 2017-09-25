package com.paper.crawler.exportUnit;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * 从百度学术的专家页面提取出专家的地址
 * @author yhj
 *
 */
public class ParseExpertUnitHtmlMap extends Mapper<Object, Text, Text, Text> {
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] strs = value.toString().replace("'", "").split(" ", 6);
		String code = strs[4];
		String html = strs[5];
		if(code.equals("01")){
			Document doc = Jsoup.parse(html);// 解析成Document对象
			Elements dElements = doc.getElementsByClass("p_affiliate");// 得到专家单位所在的p标签
			if (dElements != null && dElements.size() >= 1) {
				String unit = dElements.get(0).text();
				if (unit != null) {
					strs[5] = unit;
					String result = StringUtils.join(strs, "'");
					context.write(new Text(result), new Text(""));
				}
			}
		}
		if(code.equals("02")){
			Document doc = Jsoup.parse(html);// 解析成Document对象
			Element element = doc.getElementById("kw");
			if(element != null){
				String[] info = element.attr("value").split(" ");
				if(info.length > 1){
					strs[5] = info[1];
					String result = StringUtils.join(strs, "'");
					context.write(new Text(result), new Text(""));
				}
			}
		}
	}
}
