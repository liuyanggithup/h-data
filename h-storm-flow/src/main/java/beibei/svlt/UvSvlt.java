package beibei.svlt;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import beibei.dao.HBaseDAO;
import beibei.dao.imp.HBaseDAOImp;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import tools.CommonUtil;
import tools.DateFmt;

public class UvSvlt extends HttpServlet {


	private HBaseDAO dao;

	public String initData = "[]";

	public String initHisData = "[]";

	public String initHourData = "[]";
	
	private String jsonStr = "";

	private String todayStr = null;

	private String xValue = "0";

	private String xTitle = "0";

	private String uv = "0";

	String [] colsArr = new String[] { "time_title", "xValue", "uv" } ;
	
	@Override
	public void init() {
		try {
			dao = new HBaseDAOImp();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/** 跨天参数 */
		todayStr = DateFmt.getCountDate(null, DateFmt.date_short);
		/** 每次刷新页面更新数据 */
		initStrData();
	}

	private void initStrData() {
		//初始化历史点，包括当天和上月
		try {
			String rowkey_today = todayStr	.replaceAll("-", "");

			String rowkey_his = DateFmt.getCountDate(null, DateFmt.date_short, -1) .replaceAll("-", "");

			/** 读取线图当天数据 */
			List<Result> listToday = dao.getRows(
					"uv_table", rowkey_today,
					colsArr);
			initData = CommonUtil.transformHistoryData(listToday, colsArr);

			/** 读取上月同比数据 */
			List<Result> listYc = dao.getRows(
					"uv_table", rowkey_his,
					colsArr);
			initHisData = CommonUtil.transformHistoryData(listYc, colsArr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		this.doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		try {
			System.out.println("--------------dopost--------------");
			
			initHourData = this.getData(todayStr+"_hour", dao);
			
			// 读取历史点
			request.setCharacterEncoding("utf-8");
			response.setContentType("text/html;charset=utf-8");
			initStrData();

			jsonStr = "{\'initData\':" + initData + ",\'initHisData\':"
					+ initHisData + ",\'initHourData\':'"
					+ initHourData + "'}";
			
			response.getWriter().write(
					"<script type=\"text/javascript\">parent.msg" + "(\""
							+ jsonStr + "\")</script>");
			response.flushBuffer();
			
			System.out.println("initHourData:"+initHourData);
			System.out.println("initData:"+initData);
			System.out.println("initHisData:"+initHisData);
			while (true) {
				// 每次循环追加一个点
				// 判断是否跨天
				if (! DateFmt.getCountDate(null, DateFmt.date_short)
						.equals(todayStr)) {
					// 跨天初始化
					init();
					jsonStr = "{\'initData\':" + initData + ",\'initHisData\':"
							+ initHisData + ",\'isNewDay\':" + 1 + ",\'initHourData\':'"
							+ initHourData + "'}";

					response.setContentType("text/html;charset=utf-8");
					response.getWriter().write(
							"<script type=\"text/javascript\">parent.msg"
									+ "(\"" + jsonStr + "\")</script>");
					response.flushBuffer();
				}

				/** 读取hbase数据 */
				String[] valueArr = null;
				valueArr = CommonUtil.findLineData(dao,
						"uv_table", todayStr + "_lastest",
						colsArr);
				if (valueArr != null) {
					xTitle = valueArr[0];
					xValue = valueArr[1];
					uv = valueArr[2];
				}

				//加点
//				initData = CommonUtil.appendStr(initData, colsArr);

				jsonStr = "{\'name\':\'" + xTitle + "\',\'x\':" + xValue
						+ ",\'y\':" + uv + ",\'initHourData\':'"
						+ initHourData+ "'}";
				
				/** 判断一下如果值不为null才sendMsg */
				if (uv != null) {
					boolean b = sendMsg(jsonStr, response, "msg"); 
					if (b) {// 页面已被关闭，加点异常，此方法结束
						break;
					}
				}
				// 5秒推送一次数据
				Thread.sleep(5000);
				initHourData = this.getData(todayStr+"_hour", dao);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 以下方法的意思是将msg打到前台页面调用前台的“function msg(m)”方法进行显示
	protected boolean sendMsg(String msg, HttpServletResponse response,
			String javascriptMethod) {
		try {
			response.setContentType("text/html;charset=utf-8");
			response.getWriter().write(
					"<script type=\"text/javascript\">parent."
							+ javascriptMethod + "(\"" + msg + "\")</script>");
			response.flushBuffer();

		} catch (Exception e) {
			System.out.println("svlt long connect time error.....");
			return true;
		}
		return false;
	}
	public String getData(String date, HBaseDAO dao)
	{
		List<Result> list =  dao.getRows("uv_table", date) ;
		String result = "[" ;
		for(Result rs : list)
		{
			for(KeyValue keyValue : rs.raw())
			{
				if("uv".equals(new String(keyValue.getQualifier())))
				{
					result += new String(keyValue.getValue())+",";
					break;
				}
			}
		}
		result = result.substring(0, result.length() -1 ) +"]" ;
		return result;
		
	}
	@Override
	public void destroy() {
		super.destroy();
	}
}
