package beibei.svlt;

import beibei.dao.HBaseDAO;
import beibei.dao.imp.HBaseDAOImp;
import beibei.vo.AreaVo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.utils.Utils;
import tools.DateFmt;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;


public class AreaSvlt extends HttpServlet {


    HBaseDAO dao = null;
    String today = null;
    String hisDay = null;
    String hisData = null;

    /**
     * 初始化Servlet
     *
     * @throws ServletException
     */
    public void init() throws ServletException {
        dao = new HBaseDAOImp();
        //获取当天日期
        today = DateFmt.getCountDate(null, DateFmt.date_short);
    }

    public void destroy() {
        super.destroy();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        this.doPost(request, response);
    }

    /**
     * POST请求
     *
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // 取昨天
        hisDay = DateFmt.getCountDate(null, DateFmt.date_short, -1);
        System.out.println("hisDay=" + hisDay);
        //取昨天数据
        hisData = this.getData(hisDay, dao);
        System.out.println("hisData=" + hisData);
        while (true) {
            //取当前日期
            String dateStr = DateFmt.getCountDate(null, DateFmt.date_short);
            //跨天处理
            if (!dateStr.equals(today)) {
                today = dateStr;
            }
            //每个3s查询一次hbase
            String data = this.getData(today, dao);
            //拼装数据todayData:123,hisData:456
            String jsDataString = "{\'todayData\':" + data + ",\'hisData\':" + hisData + "}";

            boolean flag = this.sentData("jsFun", response, jsDataString);
            if (!flag) {
                break;
            }
            Utils.sleep(3000);
        }
    }

    /**
     * 将数据响应给页面
     *
     * @param jsFun
     * @param response
     * @param data
     * @return
     */
    public boolean sentData(String jsFun, HttpServletResponse response, String data) {
        try {
            response.setContentType("text/html;charset=utf-8");
            response.getWriter().write("<script type=\"text/javascript\">parent." + jsFun + "(\"" + data + "\")</script>");
            response.flushBuffer();
            return true;
        } catch (Exception e) {
            System.out.println(" long connect 已断开 ");
            return false;
        }
    }

    /**
     * 获取数据
     *
     * @param date
     * @param dao
     * @return
     */
    public String getData(String date, HBaseDAO dao) {
        List<Result> list = dao.getRows("area_order", date, new String[]{"order_amt"});
        AreaVo vo = new AreaVo();
        for (Result rs : list) {
            String rowKey = new String(rs.getRow());
            String areaId = null;
            if (rowKey.split("_").length == 2) {
                areaId = rowKey.split("_")[1];
            }
            vo.setData(areaId, new String(rs.value()));
        }
        String result = "[" + getFmtPoint(vo.getBeijing()) + "," + getFmtPoint(vo.getShanghai()) + "," + getFmtPoint(vo.getGuangzhou()) + "," + getFmtPoint(vo.getShenzhen()) + "," + getFmtPoint(vo.getChengdu()) + "]";
        return result;

    }

    /**
     * 字符串转换
     *
     * @param str
     * @return
     */
    public String getFmtPoint(String str) {
        DecimalFormat format = new DecimalFormat("#");
        if (str != null) {
            return format.format(Double.parseDouble(str));
        }
        return null;
    }


}
