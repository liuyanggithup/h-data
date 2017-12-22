package hbase.state;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import tools.DateFmt;

public class MySplit extends BaseFunction {


    String patton = null;

    public MySplit(String patton) {
        this.patton = patton;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String log = tuple.getString(0);
        String logArr[] = log.split(patton);
        if (logArr.length == 3) {
            collector.emit(new Values(DateFmt.getCountDate(logArr[2], DateFmt.date_short), "cf", "pv_count", logArr[1]));
        }


    }
}
