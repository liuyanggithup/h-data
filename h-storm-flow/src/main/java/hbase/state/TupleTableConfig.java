package hbase.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TupleTableConfig implements Serializable {

    protected String tupleRowKeyField;
    protected String tupleTimestampField;
    protected Map<String, Set<String>> columnFamilies;
    private String tableName;

    public TupleTableConfig(final String table) {
        this.tableName = table;
        this.tupleTimestampField = "";
        this.columnFamilies = new HashMap<String, Set<String>>();
    }

    public TupleTableConfig(final String table, final String rowkeyField, final String tupleTimestampField) {
        this.tableName = table;
        this.tupleRowKeyField = rowkeyField;
        this.tupleTimestampField = tupleTimestampField;
        this.columnFamilies = new HashMap<String, Set<String>>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTupleRowKeyField() {
        return tupleRowKeyField;
    }

    public void setTupleRowKeyField(String tupleRowKeyField) {
        this.tupleRowKeyField = tupleRowKeyField;
    }

    public String getTupleTimestampField() {
        return tupleTimestampField;
    }

    public void setTupleTimestampField(String tupleTimestampField) {
        this.tupleTimestampField = tupleTimestampField;
    }

    public Map<String, Set<String>> getColumnFamilies() {
        return columnFamilies;
    }

    public void setColumnFamilies(Map<String, Set<String>> columnFamilies) {
        this.columnFamilies = columnFamilies;
    }


}
