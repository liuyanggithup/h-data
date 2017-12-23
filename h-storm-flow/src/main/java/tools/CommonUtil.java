package tools;

import java.util.List;

import beibei.dao.HBaseDAO;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;


public class CommonUtil {

	
	
	public static String transformHistoryData(List<Result> listToday,
			String[] colsArr) {
		// json_str_="'[\{name:\\'name1\\',x:13.28,y:0\},\{name:\\'name2\\',x:13.68,y:5\}]'";
		// var option_={name:'name',x:a,y:b};
		StringBuffer data = new StringBuffer();
		data.append("\'[");
		int oNum = 0;
		for (Result r : listToday) {
			oNum++;
			String p_title = null;
			String xValue = null;
			String yValue = null;
			for (KeyValue keyValue : r.raw()) {
				if (new String(keyValue.getQualifier()).equals(colsArr[0] )) {
					p_title = new String(keyValue.getValue());
				}
				if (new String(keyValue.getQualifier()).equals(colsArr[1])) {
					xValue = new String(keyValue.getValue());
				}
				if (new String(keyValue.getQualifier()).equals(colsArr[2])) {
					yValue = new String(keyValue.getValue());
				}
			}
			if (oNum != 1) {
				data.append(",");
			}
			data.append(getOnePointJson(p_title,xValue,yValue));
		}
		data.append("]\'");
		return data.toString();
	}
	
	/**
	 * 得到一个点的json串，如：{name:'name',x:a,y:b}
	 */
	public static String getOnePointJson(String pt,String xValue,String yValue) {
		StringBuffer data = new StringBuffer();
		data.append("\\{");
		data.append("name:\\\\'" + pt + "\\\\'");
		data.append(",x:" + xValue);
		data.append(",y:" + yValue);
		data.append("\\}");
		return data.toString();
	}
	
	public static String[] findLineData(HBaseDAO dao, String tableName, String rowKeyRegexPoint, String[] colNameArr) {
		String xTitle = "";
		String xValue = "";
		String pointData = "";
		String[] valueArr = new String[3];
		try {
			// 点
			Result resultPoint = dao.getOneRow(
					tableName, rowKeyRegexPoint);
			// 一个result里有多列，取需要的列
			for (KeyValue keyValue : resultPoint.raw()) {
				if (colNameArr[0].equals(new String(keyValue.getQualifier()))) {
					xTitle = new String(keyValue.getValue());
				}
				if (colNameArr[1].equals(new String(keyValue.getQualifier()))) {
					xValue = new String(keyValue.getValue());
				}
				if (colNameArr[2].equals(new String(keyValue.getQualifier()))) {
					pointData = new String(keyValue.getValue());
				}
			}
			valueArr[0] = xTitle;
			valueArr[1] = xValue;
			valueArr[2] = pointData;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return valueArr;
	}
	
	public static String appendStr(String sBuffer, String[] arr) {
		if (sBuffer.toString().equals("'[]'")) {
			sBuffer = sBuffer.substring(0, sBuffer.length() - 2);
			return sBuffer + getOnePointJson(arr[0],arr[1],arr[2]) + "]\'";
		} else {
			sBuffer = sBuffer.substring(0, sBuffer.length() - 2);
			return sBuffer + "," + getOnePointJson(arr[0],arr[1],arr[2]) + "]\'";
		}
	}
	
}
