package beibei.bolt;

import beibei.dao.HBaseDAO;
import beibei.dao.imp.HBaseDAOImp;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import tools.DateFmt;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


public class UVRsltBolt implements IBasicBolt {

	/**
	 * 单线程，全局汇总
	 */
	private static final long serialVersionUID = 1L;
	HBaseDAO dao = null;
	@Override
	public void cleanup() {

	}
	long beginTime = System.currentTimeMillis() ;
	long endTime = 0;
	int hour = 0;
	long hour_uv = 0;
	// 日期，非跳出UV数
	Map<String, Long> uvMap = new HashMap<>() ;
	String todayStr = null;
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input != null) {
			String key = input.getString(0);
			String dateStr = key.split("_")[0] ;
			if (todayStr != dateStr
					&& todayStr.compareTo(dateStr) < 0) {
				//跨天处理
				uvMap.clear() ;
			}
			//判断是否跨小时
			
			if (Calendar.getInstance().get(Calendar.HOUR_OF_DAY) != hour) {
				//跨小时处理
				hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
				hour_uv = 0;
			}
			
			Long uvCnt = uvMap.get(dateStr);
			if (uvCnt == null) {
				uvCnt = 0L ;
			}
			uvCnt ++ ;
			hour_uv ++ ;
			uvMap.put(dateStr, uvCnt) ;
			//定时写库
			endTime = System.currentTimeMillis() ;
			if (endTime - beginTime >= 5000) {
				System.err.println("非跳出uv="+uvCnt);
				// 5s 写一次库
				// 写库的列
				String arr[] = this.getXValueStr() ;
				String rowkey = DateFmt.getCountDate(null, DateFmt.date_minute) ;//到分钟级
				//保存历史点，为了去月环比 ，可以每分钟写一次
				dao.insert("uv_table", rowkey, "cf", new String[]{"time_title","xValue","uv"}, new String[]{arr[0],arr[1],uvMap.get(todayStr)+"" } ) ;
				//用于实时刷新
				dao.insert("uv_table", todayStr+"_lastest", "cf", new String[]{"time_title","xValue","uv"}, new String[]{arr[0],arr[1],uvMap.get(todayStr)+"" } ) ;
				
				//小时级柱图数据
				dao.insert("uv_table", todayStr+"_hour_"+hour, "cf", new String[]{"uv"}, new String[]{hour_uv+"" } ) ;
				beginTime = System.currentTimeMillis() ;
			}
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		dao = new HBaseDAOImp() ;
		todayStr = DateFmt.getCountDate(null, DateFmt.date_short) ;
		Calendar calendar = Calendar.getInstance() ;
		hour = calendar.get(Calendar.HOUR_OF_DAY);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	//获取X坐标
	public String[] getXValueStr() {
		Calendar c = Calendar.getInstance();
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
	
}
