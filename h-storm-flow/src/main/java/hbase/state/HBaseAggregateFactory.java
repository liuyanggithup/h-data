package hbase.state;


import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class HBaseAggregateFactory implements StateFactory {

    private StateType type;
    private TridentConfig config;

    public HBaseAggregateFactory(TridentConfig config, StateType type) {
        this.config = config;
        this.type = type;

        if (config.getStateSerializer() == null) {
            config.setStateSerializer(TridentConfig.DEFAULT_SERIALIZES.get(type));
        }
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics,
                           int partitionIndex, int numPartitions) {

        HBaseAggregateState state = new HBaseAggregateState(config);
        CachedMap c = new CachedMap(state, config.getStateCacheSize());

        MapState ms = null;
        if (type == StateType.NON_TRANSACTIONAL) {
            ms = NonTransactionalMap.build(c);

        } else if (type == StateType.OPAQUE) {
            ms = OpaqueMap.build(c);
        } else if (type == StateType.TRANSACTIONAL) {
            ms = TransactionalMap.build(c);
        }
        return new SnapshottableMap(ms, new Values("$GLOBAL$"));
    }
}
