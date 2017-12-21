package beibei.trident.drpc;


import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class TridentDRPCclient {


    public static void main(String[] args) throws Exception {

        Map config = Utils.readDefaultConfig();
        DRPCClient client = new DRPCClient(config, "192.168.25.102", 3772);
        //LocalDRPC client = new LocalDRPC();
        try {
            while (true) {
                System.err.println("销售额：" + client.execute("getOrderAmt", "2014-09-13:cf:amt_5 2014-09-13:cf:amt_8"));
                System.err.println("订单数：" + client.execute("getOrderNum", "2014-09-13:cf:amt_1 2014-09-13:cf:amt_2"));
                Utils.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
