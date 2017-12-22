package hbase.state;

import org.apache.storm.trident.state.*;

import java.util.HashMap;
import java.util.Map;

public class TridentConfig<T> extends TupleTableConfig {

    public static final Map<StateType, Serializer> DEFAULT_SERIALIZES
            = new HashMap<StateType, Serializer>() {
        {
            put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
            put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
            put(StateType.OPAQUE, new JSONOpaqueSerializer());
        }

    };
    private int stateCacheSize = 1000;
    private Serializer stateSerializer;

    public TridentConfig(String table) {
        super(table);
    }

    public TridentConfig(String table, String rowkeyField, String tupleTimestampField) {
        super(table, rowkeyField, tupleTimestampField);
    }

    public int getStateCacheSize() {
        return stateCacheSize;
    }

    public void setStateCacheSize(int stateCacheSize) {
        this.stateCacheSize = stateCacheSize;
    }

    public Serializer getStateSerializer() {
        return stateSerializer;
    }

    public void setStateSerializer(Serializer stateSerializer) {
        this.stateSerializer = stateSerializer;
    }
}
