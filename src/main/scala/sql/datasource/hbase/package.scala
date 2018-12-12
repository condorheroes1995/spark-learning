package sql.datasource

import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory

package object hbase {

  private[hbase] val LOG = LoggerFactory.getLogger("hbase")

  private[hbase] abstract class SchemaField extends Serializable

  private[hbase] case class RegisteredSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable {
    override def toString: String = s"[fieldName: $fieldName, fieldType: $fieldType]"
  }

  private[hbase] case class HBaseSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable {
    override def toString: String = s"[fieldName: $fieldName, fieldType: $fieldType]"
  }

  private[hbase] case class SchemaType(dataType: DataType, nullable: Boolean)

  private[hbase] val SPARK_SQL_TABLE_SCHEMA  = "sparksql.table.schema"
  private[hbase] val HBASE_ZOOKEEPER_QUORUM  = HConstants.ZOOKEEPER_QUORUM
  private[hbase] val HBASE_TABLE_NAME        = "hbase.table.name"
  private[hbase] val HBASE_TABLE_SCHEMA      = "hbase.table.schema"
  private[hbase] val HBASE_ROOTDIR           = "hbase.rootdir"
  private[hbase] val HBASE_SNAPSHOT_VERSIONS = "hbase.snapshot.versions"
  private[hbase] val HBASE_SNAPSHOT_PATH     = "hbase.snapshot.path"
  private[hbase] val HBASE_SNAPSHOT_NAME     = "hbase.snapshot.name"

  private[hbase] val defaultSnapshotVersions = "3"
  private[hbase] val defaultHBaseRootDir     = "/hbase"

  /**
    * Adds a method, `hbaseTable`, to SQLContext that allows reading data stored in hbase table.
    */
  implicit class HBaseSnapshotContext(sqlContext: SQLContext) {
    def hbaseTable(sparksqlTableSchema: String,
                   zookeeperQuorum: String,
                   hbaseTableName: String,
                   hbaseTableSchema: String,
                   hbaseRootDir: String = defaultHBaseRootDir,
                   hbaseSnapshotVersions: String = defaultSnapshotVersions,
                   hbaseSnapshotPath: String,
                   hbaseSnapshotName: String) = {
      sqlContext.baseRelationToDataFrame(HBaseSnapshotRelation(Map(
        SPARK_SQL_TABLE_SCHEMA -> sparksqlTableSchema,
        HBASE_ZOOKEEPER_QUORUM -> zookeeperQuorum,
        HBASE_TABLE_NAME -> hbaseTableName,
        HBASE_TABLE_SCHEMA -> hbaseTableSchema,
        HBASE_ROOTDIR -> hbaseRootDir,
        HBASE_SNAPSHOT_VERSIONS -> hbaseSnapshotVersions,
        HBASE_SNAPSHOT_PATH -> hbaseSnapshotPath,
        HBASE_SNAPSHOT_NAME -> hbaseSnapshotName
      ))(sqlContext))
    }
  }

}
