package beibei.dao.imp;



import beibei.dao.HBaseDAO;
import tools.DateFmt;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Bushu2Hbase {


	
	//造数据用，为了展示完整图形
	public static String[] getXValueStrData(Calendar c) {
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		//总秒数
		int curSecNum = hour * 3600 + minute * 60 + sec;

		Double xValue = (double) curSecNum / 3600;
		// 时间，数值
		String[] end = { hour + ":" + minute, xValue.toString() };
		return end;
	}
	
	public static void main(String[] args) throws Exception {
		//补数代码

		HBaseDAO dao = new HBaseDAOImp() ;
		dao.deleteRecords("uv_table", "20141016");
		
		
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DateFmt.date_short) ;
		SimpleDateFormat sdf2 = new SimpleDateFormat(DateFmt.date_minute) ;
		calendar.setTime(sdf.parse("2014-10-16"));
		long uv=0;
		for (int i = 0; i < 21; i++) {
			calendar.add(Calendar.MINUTE, 60);
			String rowkey = sdf2.format(calendar.getTime()) ;//到分钟级
			uv = uv + Math.round(Math.random()*10000) ;
			System.out.println(rowkey);
//			System.out.println(calendar.getTime());
			String arr[] = getXValueStrData(calendar);
			System.out.println("time_title:"+arr[0]+";xValue:"+arr[1]+";uv:"+uv);
			dao.insert("uv_table", rowkey, "cf", new String[]{"time_title","xValue","uv"}, new String[]{arr[0],arr[1],uv+"" } ) ;
		}
		
		
		
		
		
		
	}

}
