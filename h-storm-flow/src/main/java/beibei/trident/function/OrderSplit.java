package beibei.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import tools.DateFmt;

public class OrderSplit extends BaseFunction {


    String patten = null;

    public OrderSplit(String patten) {
        this.patten = patten;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        if (!tuple.isEmpty()) {
            String msg = tuple.getString(0);
            // order_id,order_amt,create_time,province_id
            String value[] = msg.split(this.patten);
            System.err.println("OrderSplit-->msg=" + msg);
            //"order_id", "order_amt", "create_date", "province_id"
            collector.emit(new Values(value[0], Double.parseDouble(value[1]), DateFmt.getCountDate(value[2], DateFmt.date_short), value[3]));
        }
    }


}
