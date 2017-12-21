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
            String value[] = msg.split(this.patten);
            System.err.println("msg=" + msg);
            collector.emit(new Values(value[0], Double.parseDouble(value[1]), DateFmt.getCountDate(value[2], DateFmt.date_short), value[3]));

        }
    }


}
