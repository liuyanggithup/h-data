package hbase.state;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseAggregateState<T> implements IBackingMap<T> {

    private HTableConnector connector;
    private Serializer<T> serializer;

    public HBaseAggregateState(TridentConfig config) {
        this.serializer = config.getStateSerializer();
        try {
            this.connector = new HTableConnector(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static StateFactory opaque(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.OPAQUE);
    }

    public static StateFactory transactional(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.TRANSACTIONAL);
    }

    public static StateFactory nonTransactional(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.NON_TRANSACTIONAL);
    }


    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<Get> gets = new ArrayList<Get>(keys.size());
        byte[] rk;
        byte[] cf;
        byte[] cq;
        for (List<Object> k : keys) {
            rk = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(0));
            cf = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(1));
            cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(2));
            System.err.println(new String(rk));
            System.err.println(new String(cf));
            System.err.println(new String(cq));
            Get get = new Get(rk);
            gets.add(get.addColumn(cf, cq));
        }

        org.apache.hadoop.hbase.client.Result[] results = null;
        try {
            results = connector.getTable().get(gets);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<T> rtn = new ArrayList<T>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            cf = org.apache.hadoop.hbase.util.Bytes.toBytes((String) keys.get(i).get(1));
            cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String) keys.get(i).get(2));
            org.apache.hadoop.hbase.client.Result result = results[i];
            if (result.isEmpty()) {
                rtn.add(null);
            } else {
                rtn.add((T) serializer.deserialize(result.getValue(cf, cq)));
            }

        }
        return rtn;
    }

    @Override
    public void multiPut(List<List<Object>> k, List<T> vals) {
        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < k.size(); i++) {
            byte[] rk = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(i).get(0));
            byte[] cf = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(i).get(1));
            byte[] cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String) k.get(i).get(2));

            System.err.println(new String(rk));
            System.err.println(new String(cf));
            System.err.println(new String(cq));

            byte[] cv = serializer.serialize(vals.get(i));
            Put p = new Put(rk);
            puts.add(p.addColumn(cf, cq, cv));
        }

        try {
            connector.getTable().put(puts);
            connector.getTable().close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
