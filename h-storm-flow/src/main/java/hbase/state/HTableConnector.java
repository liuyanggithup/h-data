package hbase.state;

import kafka.productor.KafkaProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;

public class HTableConnector implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    Connection connection = null;
    private Configuration configuration;
    private Admin admin;
    private String tableName;
    private Table table;

    public HTableConnector(TupleTableConfig conf) throws Exception {

        configuration = HBaseConfiguration.create();
        String zk_list = KafkaProperties.zkConnect;
        configuration.set("hbase.zookeeper.quorum", zk_list);
        try {

            String os = System.getProperty("os.name");
            if(os.toLowerCase().startsWith("win")){
                System.setProperty("hadoop.home.dir", "E:\\gitpro\\HADOOP_HOME");
            }

            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.tableName = conf.getTableName();
        this.admin = connection.getAdmin();
        this.table = connection.getTable(TableName.valueOf(tableName));
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void close() {
        try {
            this.table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
