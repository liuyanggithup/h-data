package beibei.trident.function;


import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Split extends BaseFunction {

    String patten = null;

    public Split(String patten) {
        this.patten = patten;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        if (!tuple.isEmpty()) {
            String msg = tuple.getString(0);
            System.out.println("*************"+msg+"*************");
            String value[] = msg.split(this.patten);
            for (String v : value) {
                collector.emit(new Values(v));
            }
        }
    }


}
