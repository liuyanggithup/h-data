package beibei.svlt;

import beibei.dao.HBaseDAO;
import beibei.dao.imp.HBaseDAOImp;
import beibei.vo.AreaVo;
import org.apache.hadoop.hbase.KeyValue;
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

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    HBaseDAO dao = null;

    String today = null;
    String hisDay = null;
    String hisData = null;

    public void init() throws ServletException {
        dao = new HBaseDAOImp();
        today = DateFmt.getCountDate(null, DateFmt.date_short);
    }

    public void destroy() {
        super.destroy();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        this.doPost(request, response);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        hisDay = DateFmt.getCountDate(null, DateFmt.date_short, -1);// 取昨天
        System.out.println("hisDay=" + hisDay);
        hisData = this.getData(hisDay, dao);
        System.out.println("hisData=" + hisData);
        while (true) {
            String dateStr = DateFmt.getCountDate(null, DateFmt.date_short);
            if (!dateStr.equals(today)) {
                //跨天处理
                today = dateStr;
            }
            //每个3s查询一次hbase
            String data = this.getData(today, dao);
            //todayData:123,hisData:456
            String jsDataString = "{\'todayData\':" + data + ",\'hisData\':" + hisData + "}";

            boolean flag = this.sentData("jsFun", response, jsDataString);
            if (!flag) {
                break;
            }
            Utils.sleep(3000);
        }
    }

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

    public String getData(String date, HBaseDAO dao) {
        List<Result> list = dao.getRows("area_order", date);
        AreaVo vo = new AreaVo();
        for (Result rs : list) {
            String rowKey = new String(rs.getRow());
            String aredid = null;
            if (rowKey.split("_").length == 2) {
                aredid = rowKey.split("_")[1];
            }
            for (KeyValue keyValue : rs.raw()) {
                if ("order_amt".equals(new String(keyValue.getQualifier()))) {
                    vo.setData(aredid, new String(keyValue.getValue()));
                    break;
                }
            }
        }
        String result = "[" + getFmtPoint(vo.getBeijing()) + "," + getFmtPoint(vo.getShanghai()) + "," + getFmtPoint(vo.getGuangzhou()) + "," + getFmtPoint(vo.getShenzhen()) + "," + getFmtPoint(vo.getChengdu()) + "]";
        return result;

    }

    public String getFmtPoint(String str) {
        DecimalFormat format = new DecimalFormat("#");
        if (str != null) {
            return format.format(Double.parseDouble(str));
        }
        return null;
    }


}
