package beibei.trident.function;


import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class Print extends BaseFunction {


    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        if (!tuple.isEmpty()) {
            String msg = tuple.getString(0);
            System.err.println(msg);
        }
    }


}
