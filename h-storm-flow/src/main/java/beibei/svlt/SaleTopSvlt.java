package beibei.svlt;

import beibei.trident.function.Province;
import kafka.productor.KafkaProperties;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import tools.DateFmt;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SaleTopSvlt extends HttpServlet {

    String today = null;

    public void init() throws ServletException {
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
        request.setCharacterEncoding("utf-8");
        response.setContentType("text/html;charset=utf-8");

        /**
         *
         * [["2014-08-19:cf:amt_1 2014-08-19:cf:amt_2","2014-08-19:cf:amt_1","2014-08-19","cf","amt_1",1842.7999999999995],
         * ["2014-08-19:cf:amt_1 2014-08-19:cf:amt_2","2014-08-19:cf:amt_2","2014-08-19","cf","amt_2",1773.499999999999]]
         *
         *
         */
        Map<String, String> provMap = Province.getProvMap();
        // 每个5s查询
        Map config = Utils.readDefaultConfig();
        DRPCClient client = null;
        try {
            client = new DRPCClient(config, KafkaProperties.hbase_zkList, 3772);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            while (true) {
                String todayColumn = "[";//柱子图value
                String todaySpline = "[";//折线图value

                String xName = "[";
                String dateStr = DateFmt.getCountDate(null, DateFmt.date_short);

                String amtArgs = this.getArgs("amt", dateStr, provMap);
                String saleStr = client.execute("getOrderAmt", amtArgs);

                System.err.println("销售额：" + saleStr);
                String amtArr[] = saleStr.split("\\],\\[");
                System.out.println(amtArr[4]);
                List<String> top5List = new ArrayList<String>();
                for (String amt : amtArr) {
                    System.out.println("amt=" + amt);
                    String provId = amt.split(",")[4].replaceAll("\"", "").split("_")[1];
                    top5List.add(provId);
                    xName += provMap.get(provId) + ",";
                    todayColumn += getFmtPoint(amt.split(",")[5].replaceAll("\\]\\]", "")) + ",";
                }
                xName = xName.substring(0, xName.length() - 1) + "]";
                System.out.println(xName);

                todayColumn = todayColumn.substring(0, todayColumn.length() - 1) + "]";
                System.err.println("todayColumn=" + todayColumn);


                String orderNumArgs = this.getArgs("orderNum", dateStr, provMap);
                String orderStr = client.execute("getOrderNum", orderNumArgs);
                //订单数，需要取出销售额前5名的
                System.err.println("订单数：" + orderStr);
                String orderArr[] = orderStr.split("\\],\\[");
                Map<String, String> orderMap = new HashMap<String, String>();
                for (String order : orderArr) {
                    String provId = order.split(",")[4].replaceAll("\"", "").split("_")[1];
                    orderMap.put(provId, order.split(",")[5].replaceAll("\\]\\]", ""));
                }
                for (String provId : top5List) {
                    todaySpline += orderMap.get(provId) + ",";
                }
                todaySpline = todaySpline.substring(0, todaySpline.length() - 1) + "]";
                System.err.println("todaySpline=" + todaySpline);

                Utils.sleep(5000);

                String jsDataString = "{\'todayColumn\':" + todayColumn + ",\'todaySpline\':" + todaySpline
                        + ",\'xName\':\'" + xName
                        + "\'}";

                boolean flag = this.sentData("jsFun", response, jsDataString);
                if (!flag) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean sentData(String jsFun, HttpServletResponse response,
                            String data) {
        try {
            response.setContentType("text/html;charset=utf-8");
            response.getWriter().write(
                    "<script type=\"text/javascript\">parent." + jsFun + "(\""
                            + data + "\")</script>");
            response.flushBuffer();
            return true;
        } catch (Exception e) {
            System.out.println(" long connect 已断开 ");
            return false;
        }
    }

    public String getArgs(String flag, String dateStr, Map<String, String> provMap) {
        String args = "";
        //2014-08-20:cf:amt_1
        for (String str : provMap.keySet()) {
            args += dateStr + ":cf:" + flag + "_" + str + " ";
        }
        if (args != null) {
            args = args.trim();
        }
        return args;
    }

    public String getFmtPoint(String str) {
        DecimalFormat format = new DecimalFormat("#");
        if (str != null) {
            return format.format(Double.parseDouble(str));
        }
        return null;
    }

}
