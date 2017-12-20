package beibei.dao.imp;

import beibei.dao.HBaseDAO;
import kafka.productor.KafkaProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HBaseDAOImp implements HBaseDAO {

    static Admin admin;
    static Connection connection;

    public HBaseDAOImp() {

        Configuration conf = HBaseConfiguration.create();
        String zk_list = KafkaProperties.hbase_zkList;
        conf.set("hbase.zookeeper.quorum", zk_list);
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            System.setProperty("hadoop.home.dir", "E:\\gitpro\\HADOOP_HOME");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        HBaseDAO dao = new HBaseDAOImp();

        dao.insert("area_order", "2017-12-20_1", "cf", "order_amt", 0 + "");
        dao.insert("area_order", "2017-12-20_2", "cf", "order_amt", 0 + "");
        dao.insert("area_order", "2017-12-20_3", "cf", "order_amt", 0 + "");
        dao.insert("area_order", "2017-12-20_4", "cf", "order_amt", 0 + "");
        dao.insert("area_order", "2017-12-20_5", "cf", "order_amt", 0 + "");


//		List<Put> list = new ArrayList<Put>();
//		Put put = new Put("123456".getBytes());
//		put.addColumn("cf".getBytes(), "name".getBytes(), "zhaoliu2".getBytes()) ;
//		list.add(put) ;
//		dao.save(put, "test") ;
//        put = new Put("223456".getBytes());
//        put.addColumn("cf".getBytes(), "name".getBytes(), "zhaoliu2".getBytes()) ;
//        list.add(put) ;
//        dao.save(put, "test") ;
//		put.addColumn("cf".getBytes(), "addr".getBytes(), "shanghai1".getBytes()) ;
//		list.add(put) ;
//		put.addColumn("cf".getBytes(), "age".getBytes(), "30".getBytes()) ;
//		list.add(put) ;
//		put.addColumn("cf".getBytes(), "tel".getBytes(), "13567882341".getBytes()) ;
//		list.add(put) ;
//
//		dao.save(list, "test");
//		dao.save(put, "test") ;
//
//        List<Result> list2 = dao.getRows("test", "", new String[]{"name","addr"});
//        for (Result rs : list2) {
//            byte[] value = rs.value();
//            System.out.println(new String(rs.getRow())+"***"+new String(value));
//
//            for (KeyValue keyValue : rs.raw()) {
//                System.out.println("rowkey:" + new String(keyValue.getRow()));
//                System.out.println("Qualifier:" + new String(keyValue.getQualifier()));
//                System.out.println("Value:" + new String(keyValue.getValue()));
//                System.out.println("----------------");
//            }
//
//
//        }

//		dao.insert("test", "testrow", "cf", "age", "35") ;
//		dao.insert("test", "testrow", "cf", "cardid", "12312312335") ;
//		dao.insert("test", "testrow", "cf", "tel", "13512312345") ;
//        List<Result> list = dao.getRows("state", "2014-01", new String[]{"pv_count"});
//        for (Result rs : list) {
//            for (KeyValue keyValue : rs.raw()) {
//                System.out.println("rowkey:" + new String(keyValue.getRow()));
//                System.out.println("Qualifier:" + new String(keyValue.getQualifier()));
//                System.out.println("Value:" + new String(keyValue.getValue()));
//                System.out.println("----------------");
//            }
//        }
//		Result rs = dao.getOneRow("test", "testrow");


    }

    @Override
    public void save(Put put, String tableName) {

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(put);
        } catch (Exception e) {
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insert(String tableName, String rowKey, String family,
                       String quailifer, String value) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insert(String tableName, String rowKey, String family, String quailifer[], String value[]) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            // 批量添加
            for (int i = 0; i < quailifer.length; i++) {
                String col = quailifer[i];
                String val = value[i];
                put.addColumn(family.getBytes(), col.getBytes(), val.getBytes());
            }
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void save(List<Put> Put, String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(Put);
        } catch (Exception e) {
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public Result getOneRow(String tableName, String rowKey) {

        Result rsResult = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            rsResult = table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rsResult;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKeyLike) {

        List<Result> list = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) {

        List<Result> list = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            for (int i = 0; i < cols.length; i++) {
                scan.addColumn("cf".getBytes(), cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public List<Result> getRows(String tableName, String startRow, String stopRow) {

        List<Result> list = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rsResult : scanner) {
                list.add(rsResult);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public void deleteRecords(String tableName, String rowKeyLike) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            List<Delete> list = new ArrayList<Delete>();
            for (Result rs : scanner) {
                Delete del = new Delete(rs.getRow());
                list.add(del);
            }
            table.delete(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
