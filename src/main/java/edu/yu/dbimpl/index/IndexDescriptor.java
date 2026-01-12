package edu.yu.dbimpl.index;

import java.util.Objects;

import edu.yu.dbimpl.index.IndexMgrBase.IndexType;
import edu.yu.dbimpl.record.SchemaBase;

public class IndexDescriptor extends IndexDescriptorBase {

    private final String tableName;
    private final SchemaBase indexedTableSchema;
    private final String indexName;
    private final String fieldName;
    private final IndexType indexType;

    public IndexDescriptor(String tableName, SchemaBase indexedTableSchema, String indexName, String fieldName,
            IndexType indexType) {
        super(tableName, indexedTableSchema, indexName, fieldName, indexType);

        this.tableName = tableName;
        this.indexedTableSchema = indexedTableSchema;
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.indexType = indexType;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public SchemaBase getIndexedTableSchema() {
        return this.indexedTableSchema;
    }

    @Override
    public String getIndexName() {
        return this.indexName;
    }

    @Override
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public IndexType getIndexType() {
        return this.indexType;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("IndexDescriptor: [tableName: ")
                .append(this.tableName)
                .append(", indexedTableSchema: ")
                .append(this.indexedTableSchema)
                .append(", indexName: ")
                .append(this.indexName)
                .append(", fieldName: ")
                .append(this.fieldName)
                .append(", indexType: ")
                .append(this.indexType)
                .append("]")
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof IndexDescriptor)) {
            return false;
        }

        IndexDescriptor other = ((IndexDescriptor) o);
        return this.tableName.equals(other.tableName)
                && this.indexedTableSchema.equals(other.indexedTableSchema)
                && this.indexName.equals(other.indexName)
                && this.fieldName.equals(other.fieldName)
                && this.indexType == other.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.tableName, this.indexedTableSchema, this.indexName, this.fieldName);
    }

}
