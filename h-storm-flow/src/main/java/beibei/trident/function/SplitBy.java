package beibei.trident.function;


import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitBy extends BaseFunction {


    String patten = null;

    public SplitBy(String patten) {
        this.patten = patten;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        if (!tuple.isEmpty()) {
            String msg = tuple.getString(0);
            String value[] = msg.split(this.patten);
            collector.emit(new Values(value[0], value[1]));
        }
    }


}
