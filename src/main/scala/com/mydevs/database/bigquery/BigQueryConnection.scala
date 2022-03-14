package com.mydevs.database.bigquery

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName._
import com.google.cloud.bigquery.StandardTableDefinition.StreamingBuffer
import com.google.cloud.bigquery.TableDefinition.Type
import com.google.cloud.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Utilities for interacting with BigQuery using the java bigquery API.
 *
 * ==Overview==
 * This includes table creation, table schema update, table existence checking
 * More utilities will be added later
 */
object BigQueryConnection {

  val logger = LoggerFactory.getLogger("BigQueryConnection")

  val spark = SparkSession.builder().getOrCreate()

  val bigQuery = BigQueryOptions.getDefaultInstance.getService

  val temporaryGcsBucketName = try {
    Some(spark.conf.get("spark.bigquery.connector.temporaryGcsBucketName"))
  } catch {
    case _: Throwable => None
  }

  val archiverGcsBucketName = try {
    Some(spark.conf.get("spark.bigquery.connector.archiverGcsBucketName"))
  } catch {
    case _: Throwable => None
  }

  trait PartitionsMetadata

  case class BqRangePartitioning(start: Long, end: Long, interval: Long) extends PartitionsMetadata

  case class BqPartition(partitioningColumnName: String, partitioningColumnType: String, metaData: PartitionsMetadata)

  case class BqTimePartitioning(partitioningColumnName: String, timePartitioningType: TimePartitioning.Type, expirationMs: Long) extends PartitionsMetadata

  case class BqMaterializedView(materializedViewName: String, materializedViewDef: MaterializedViewDefinition)

  case class BqView(viewName: String, viewDef: ViewDefinition)

  case class BqExternalData(sourceUris: Option[Seq[String]],
                            schema: Option[Schema] = None,
                            formatOptions: Option[FormatOptions],
                            decimalTargetTypes: Option[Seq[String]] = None,
                            maxBadRecords: Option[Int] = None,
                            ignoreUnknownValues: Option[Boolean] = None,
                            compression: Option[String] = None,
                            connectionId: Option[String] = None,
                            autodetect: Option[Boolean] = None,
                            tableType: Option[Type] = None,
                            hivePartitioningOptions: Option[HivePartitioningOptions] = None)

  //creating big query tables
  def createTable(bigQueryDatasetName: String,
                  bigQueryTableName: String,
                  projectId: String,
                  sparkDataframeSchema: Option[StructType],
                  partitioningMetadata: Option[BqPartition] = None,
                  clusteringFields: Option[Seq[String]] = None,
                  requirePartitionFilter: Option[Boolean] = None,
                  friendlyName: Option[String] = None,
                  description: Option[String] = None,
                  location: Option[String] = None,
                  labels: Option[Map[String, String]],
                  tableType: Option[TableDefinition.Type] = None,
                  materializedView: Option[BqMaterializedView] = None,
                  columnsMetadata: Option[Seq[Map[String, String]]],
                  view: Option[BqView] = None,
                  externalDataConfiguration: Option[BqExternalData] = None,
                  encryptionConfiguration: Option[EncryptionConfiguration] = None,
                  streamingBuffer: Option[StreamingBuffer] = None,
                  snapshotTableDefinition: Option[SnapshotTableDefinition] = None,
                  numBytes: Option[Long] = None,
                  numLongTermBytes: Option[Long] = None,
                  numRows: Option[Long] = None,
                  expirationTime: Option[Long] = None,
                  bigQuerySchema: Option[Schema] = None): Unit = {

    val bqTableAndViewsData = getBqTableAndViewData(bigQueryDatasetName, bigQueryTableName, projectId,
      sparkDataframeSchema, partitioningMetadata, clusteringFields, requirePartitionFilter, friendlyName,
      description, location, labels, tableType, materializedView, columnsMetadata, view,
      externalDataConfiguration, encryptionConfiguration, streamingBuffer, snapshotTableDefinition,
      numBytes, numLongTermBytes, numRows, expirationTime, bigQuerySchema)

    try {
      val tableData = bqTableAndViewsData._1.get
      val table = tableData._1
      val tableInfo = tableData._2

      //verifying if table exists before creation
      if (!(table.isDefined && table.get.exists())) {
        bigQuery.create(tableInfo)
        logger.warn("Table created successfully")
      } else {
        logger.warn("Table already exists")
      }
    } catch {
      case e: BigQueryException => throw new Exception("Table creation failed. \n" + e.toString)
    }

    try {
      if (materializedView.isDefined) {
        val mvData = bqTableAndViewsData._2.get
        val tableMV: Option[Table] = mvData._1
        if (!(tableMV.isDefined && tableMV.get.exists())) {
          bigQuery.create(mvData._2)
          logger.warn(s"Materialized view created successfully")
        } else {
          logger.warn(s"Materialized view already exists")
        }
      }
    } catch {
      case e: Throwable => throw new Exception("Materialised view creation failed. \n" + e.toString)
    }

    try {
      if (view.isDefined) {
        val viewData = bqTableAndViewsData._3.get
        val tableView: Option[Table] = viewData._1
        if (!(tableView.isDefined && tableView.get.exists())) {
          bigQuery.create(viewData._2)
          logger.warn(s"View created successfully")
        } else {
          logger.warn(s"View already exists")
        }
      }
    } catch {
      case e: Throwable => throw new Exception("View creation failed. \n" + e.toString)
    }

    try {
      if (snapshotTableDefinition.isDefined) {
        val viewData = bqTableAndViewsData._4.get
        val tableView: Option[Table] = viewData._1
        if (!(tableView.isDefined && tableView.get.exists())) {
          bigQuery.create(viewData._2)
          logger.warn(s"Snapshot created successfully")
        } else {
          logger.warn(s"Snapshot already exists")
        }
      }
    } catch {
      case e: Throwable => throw new Exception("Snapshot creation failed. \n" + e.toString)
    }

  }

  //updating table schema
  def updateTableSchema(bigQueryDatasetName: String,
                        bigQueryTableName: String,
                        projectId: String,
                        sparkDataframeSchema: Option[StructType],
                        partitioningMetadata: Option[BqPartition] = None,
                        clusteringFields: Option[Seq[String]] = None,
                        requirePartitionFilter: Option[Boolean] = None,
                        friendlyName: Option[String] = None,
                        description: Option[String] = None,
                        location: Option[String] = None,
                        labels: Option[Map[String, String]],
                        tableType: Option[TableDefinition.Type] = None,
                        materializedView: Option[BqMaterializedView] = None,
                        columnsMetadata: Option[Seq[Map[String, String]]],
                        view: Option[BqView] = None,
                        externalDataConfiguration: Option[BqExternalData] = None,
                        encryptionConfiguration: Option[EncryptionConfiguration] = None,
                        streamingBuffer: Option[StreamingBuffer] = None,
                        snapshotTableDefinition: Option[SnapshotTableDefinition] = None,
                        numBytes: Option[Long] = None,
                        numLongTermBytes: Option[Long] = None,
                        numRows: Option[Long] = None,
                        expirationTime: Option[Long] = None,
                        bigQuerySchema: Option[Schema] = None): Unit = {
    try {

      val bqTableAndViewsData = getBqTableAndViewData(bigQueryDatasetName, bigQueryTableName, projectId,
        sparkDataframeSchema, partitioningMetadata, clusteringFields, requirePartitionFilter, friendlyName,
        description, location, labels, tableType, materializedView, columnsMetadata, view,
        externalDataConfiguration, encryptionConfiguration, streamingBuffer, snapshotTableDefinition,
        numBytes, numLongTermBytes, numRows, expirationTime, bigQuerySchema)

      val tableData = bqTableAndViewsData._1.get
      val table = tableData._1
      val tableInfo = tableData._2

      if (table.isDefined && table.get.exists()) {
        bigQuery.update(tableInfo)
        logger.warn(s"Updated $projectId.$bigQueryDatasetName.$bigQueryTableName's table schema")
      } else {
        logger.warn(s"Table's schema update failed : table $projectId.$bigQueryDatasetName.$bigQueryTableName does not exists")
      }

      //TODO add materialize view update when it is supported by google

      if (view.isDefined) {
        val viewData = bqTableAndViewsData._3.get
        val viewTable = viewData._1
        val vienTableInfo = viewData._2
        val viewName = view.get.viewName

        if (viewTable.isDefined && viewTable.get.exists()) {
          bigQuery.update(vienTableInfo)
          logger.warn(s"Updated $projectId.$bigQueryDatasetName.$viewName's view schema")
        } else {
          logger.warn(s"View's schema update failed : View $projectId.$bigQueryDatasetName.$viewName does not exists")
        }
      }

    } catch {
      case e: BigQueryException => throw new Exception("Table creation failed. \n" + e.toString)
    }
  }

  def getBqTableAndViewData(bigQueryDatasetName: String,
                            bigQueryTableName: String,
                            projectId: String,
                            sparkDataframeSchema: Option[StructType],
                            partitioningMetadata: Option[BqPartition] = None,
                            clusteringFields: Option[Seq[String]] = None,
                            requirePartitionFilter: Option[Boolean] = None,
                            friendlyName: Option[String] = None,
                            description: Option[String] = None,
                            location: Option[String] = None,
                            labels: Option[Map[String, String]] = None,
                            tableType: Option[TableDefinition.Type] = None,
                            materializedView: Option[BqMaterializedView] = None,
                            columnsMetadata: Option[Seq[Map[String, String]]] = None,
                            view: Option[BqView] = None,
                            externalDataConfiguration: Option[BqExternalData] = None,
                            encryptionConfiguration: Option[EncryptionConfiguration] = None,
                            streamingBuffer: Option[StreamingBuffer] = None,
                            snapshotTableDefinition: Option[SnapshotTableDefinition] = None,
                            numBytes: Option[Long] = None,
                            numLongTermBytes: Option[Long] = None,
                            numRows: Option[Long] = None,
                            expirationTime: Option[Long] = None,
                            //kind: Option[String],
                            //tableReference: Option[TableReference],
                            bigQuerySchema: Option[Schema] = None) = {


    val tableId = TableId.of(projectId, bigQueryDatasetName, bigQueryTableName)
    val table: Option[Table] = Option(bigQuery.getTable(tableId))

    val tableInfo: TableInfo.Builder = if (externalDataConfiguration.isDefined) {

      val externalData = externalDataConfiguration.get

      require(externalData.sourceUris.isDefined, "The sourceUris has not been defined")
      require(externalData.formatOptions.isDefined, "The formatOptions has not been defined")

      val externalTableDefinition = ExternalTableDefinition.of(externalData.sourceUris.get.asJava.get(0), externalData.formatOptions.get).toBuilder

      if (externalData.sourceUris.isDefined) {
        externalTableDefinition.setSourceUris(externalData.sourceUris.get.asJava)
      }

      if (externalData.schema.isDefined) {
        externalTableDefinition.setSchema(externalData.schema.get)
      }

      if (externalData.autodetect.isDefined) {
        externalTableDefinition.setAutodetect(externalData.autodetect.get)
      }

      if (externalData.decimalTargetTypes.isDefined) {
        externalTableDefinition.setDecimalTargetTypes(externalData.decimalTargetTypes.get.asJava)
      }

      if (externalData.maxBadRecords.isDefined) {
        externalTableDefinition.setMaxBadRecords(externalData.maxBadRecords.get)
      }

      if (externalData.ignoreUnknownValues.isDefined) {
        externalTableDefinition.setIgnoreUnknownValues(externalData.ignoreUnknownValues.get)
      }

      if (externalData.compression.isDefined) {
        externalTableDefinition.setCompression(externalData.compression.get)
      }

      if (externalData.connectionId.isDefined) {
        externalTableDefinition.setConnectionId(externalData.connectionId.get)
      }

      if (externalData.autodetect.isDefined) {
        externalTableDefinition.setAutodetect(externalData.autodetect.get)
      }

      if (externalData.tableType.isDefined) {
        externalTableDefinition.setType(externalData.tableType.get)
      }

      if (externalData.hivePartitioningOptions.isDefined) {
        externalTableDefinition.setHivePartitioningOptions(externalData.hivePartitioningOptions.get)
      }

      TableInfo.of(tableId, externalTableDefinition.build()).toBuilder

    } else {

      val tableDefinition = StandardTableDefinition.newBuilder()

      if (partitioningMetadata.isDefined) {
        val partitionMeta = partitioningMetadata.get

        partitionMeta.partitioningColumnType.toLowerCase.trim match {
          case "int64" =>
            val range = partitionMeta.metaData.asInstanceOf[BqRangePartitioning]
            val rangePartition = RangePartitioning.newBuilder().setField(partitionMeta.partitioningColumnName).setRange(RangePartitioning.Range.newBuilder().setStart(range.start).setInterval(range.interval).setEnd(range.end).build()).build()
            tableDefinition.setRangePartitioning(rangePartition)
          case "timestamp" | "date" =>
            val timePartitionMeta = partitionMeta.metaData.asInstanceOf[BqTimePartitioning]
            val timePartition = TimePartitioning.newBuilder(timePartitionMeta.timePartitioningType).setField(partitionMeta.partitioningColumnName).setExpirationMs(timePartitionMeta.expirationMs).build()
            tableDefinition.setTimePartitioning(timePartition)
          case t => throw new Exception(s"Dynamically creating table with partition type $t is not supported or not yet implemented")
        }
      }

      if (clusteringFields.isDefined) {
        tableDefinition.setClustering(Clustering.newBuilder().setFields(clusteringFields.get.asJava).build()).build()
      }

      if (location.isDefined) {
        tableDefinition.setLocation(location.get)
      }

      if (tableType.isDefined) {
        tableDefinition.setType(tableType.get)
      }

      val schema: Schema = bigQuerySchema.getOrElse(toBqSchema(sparkDataframeSchema.getOrElse(throw new Exception("Big Query table schema or Spark Dataframe schema must be provided"))))

      if (columnsMetadata.isDefined) {
        val actualColumnsMetadata = columnsMetadata.get.map(x => x("name") -> (x.get("c-type").asInstanceOf[Option[LegacySQLTypeName]],
          x.get("mode").asInstanceOf[Option[Mode]],
          x.get("description"),
          if (x.get("policy-tags").isDefined) Some(PolicyTags.newBuilder().setNames(x.get("policy-tags").get.split("|").toList.asJava).build()) else None,
          x.get("max-length"),
          x.get("precision"),
          x.get("scale"))
        ).toMap

        //helper method for populating columns values from create table specifications
        def updateField(field: Field, fieldMeta: Option[(Option[LegacySQLTypeName], Option[Mode], Option[String], Option[PolicyTags], Option[String], Option[String], Option[String])]) = {
          val meta = fieldMeta.get

          val fType: LegacySQLTypeName = if (meta._1.isDefined) meta._1.get else field.getType

          val mode: Mode = if (meta._1.isDefined) meta._2.get else field.getMode

          val fieldBuilder = fType match {
            case LegacySQLTypeName.RECORD => Field.newBuilder(field.getName, STRUCT, field.getSubFields: _*).setMode(mode)
            case _ => Field.newBuilder(field.getName, fType).setMode(mode)
          }

          if (meta._3.isDefined) {
            fieldBuilder.setDescription(if (field.getDescription.isEmpty && field.getDescription != null) meta._3.get else Seq(field.getDescription, meta._3.get).mkString("|"))
          }

          if (meta._4.isDefined) {
            fieldBuilder.setPolicyTags(meta._4.get)
          }

          if (meta._5.isDefined) {
            fieldBuilder.setMaxLength(meta._5.get.toLong)
          }

          if (meta._6.isDefined) {
            fieldBuilder.setPrecision(meta._6.get.toLong)
          }

          if (meta._7.isDefined) {
            fieldBuilder.setScale(meta._6.get.toLong)
          }
          fieldBuilder.build()
        }

        val updatedFields = schema.getFields.map(field => {

          val fieldMeta = actualColumnsMetadata.get(field.getName)
          if (fieldMeta.isDefined) updateField(field, fieldMeta) else field
        })

        tableDefinition.setSchema(Schema.of(updatedFields: _*))

      } else {
        tableDefinition.setSchema(schema)
      }

      if (streamingBuffer.isDefined) {
        tableDefinition.setStreamingBuffer(streamingBuffer.get)
      }
      if (numBytes.isDefined) {
        tableDefinition.setNumBytes(numBytes.get)
      }

      if (numRows.isDefined) {
        tableDefinition.setNumRows(numRows.get)
      }

      if (numLongTermBytes.isDefined) {
        tableDefinition.setNumLongTermBytes(numLongTermBytes.get)
      }

      if (streamingBuffer.isDefined) {
        tableDefinition.setStreamingBuffer(streamingBuffer.get)
      }

      val standardTableInfo = TableInfo.newBuilder(tableId, tableDefinition.build())

      if (requirePartitionFilter.isDefined) {
        standardTableInfo.setRequirePartitionFilter(requirePartitionFilter.get)
      }

      standardTableInfo
    }

    if (friendlyName.isDefined) {
      tableInfo.setFriendlyName(friendlyName.get)
    }

    if (description.isDefined) {
      tableInfo.setDescription(description.get)
    }

    if (labels.isDefined) {
      tableInfo.setLabels(labels.get.asJava)
    }

    if (encryptionConfiguration.isDefined) {
      tableInfo.setEncryptionConfiguration(encryptionConfiguration.get)
    }

    if (expirationTime.isDefined) {
      tableInfo.setExpirationTime(expirationTime.get).build()
    }

    val materializedViewData = if (materializedView.isDefined) {
      val materializedViewMeta = materializedView.get
      val tableIdMV = TableId.of(bigQueryDatasetName, materializedViewMeta.materializedViewName)
      val tableMV: Option[Table] = Option(bigQuery.getTable(tableIdMV))
      Option(tableMV, TableInfo.of(tableIdMV, materializedViewMeta.materializedViewDef))
    } else None

    val viewData = if (view.isDefined) {
      val tableIdView = TableId.of(bigQueryDatasetName, view.get.viewName)
      val tableView: Option[Table] = Option(bigQuery.getTable(tableIdView))
      Option(tableView, TableInfo.of(tableIdView, view.get.viewDef))
    } else None

    val snapshotData = if (snapshotTableDefinition.isDefined) {
      val tableIdSnapshot = TableId.of(bigQueryDatasetName, bigQueryTableName)
      val tableSnapshot: Option[Table] = Option(bigQuery.getTable(tableIdSnapshot))
      Option(tableSnapshot, TableInfo.of(tableIdSnapshot, snapshotTableDefinition.get.toBuilder.setBaseTableId(tableIdSnapshot).build()))
    } else None

    (Option(table, tableInfo.build()), materializedViewData, viewData, snapshotData)
  }

  def toBqSchema(sparkSchema: StructType) = {

    def sparkStructFieldToBigQueryField(dt: StructField): Field = {
      val name = dt.name
      val dtType = dt.dataType
      val mode = if (dt.nullable) Field.Mode.NULLABLE else Field.Mode.REQUIRED
      val description: String = dt.getComment().getOrElse("")

      dtType match {
        case BinaryType => Field.newBuilder(name, BYTES).setMode(Field.Mode.REPEATED).setDescription(description).build()
        case StructType(fields) => Field.newBuilder(name, STRUCT, fields.map(sparkStructFieldToBigQueryField): _*).setDescription(description).build()
        case MapType(keyType, valueType, valueContainsNull) => Field.newBuilder(name, STRUCT, Array(StructField("Key", keyType),
          StructField("value", valueType, valueContainsNull))
          .map(sparkStructFieldToBigQueryField): _*).setMode(Field.Mode.REPEATED).setDescription(description).build()
        case ArrayType(elementType, _) => elementType match {
          case ArrayType(_, _) => throw new Exception("Array of array are not supported")
          case BinaryType => Field.newBuilder(name, BYTES).setMode(Field.Mode.REPEATED).setDescription(description).build()
          case StructType(fields) => Field.newBuilder(name, STRUCT, fields.map(sparkStructFieldToBigQueryField): _*).setMode(Field.Mode.REPEATED).setDescription(description).build()
          case MapType(keyType, valueType, valueContainsNull) => Field.newBuilder(name, STRUCT, Array(StructField("Key", keyType),
            StructField("value", valueType, valueContainsNull))
            .map(sparkStructFieldToBigQueryField): _*).setMode(Field.Mode.REPEATED).setDescription(description).build()
          case _ => Field.newBuilder(name, toBqTypeNameWithComments(elementType)).setMode(Field.Mode.REPEATED).setDescription(description).build()
        }
        case _ => Field.newBuilder(name, toBqTypeNameWithComments(dtType))
          //.setMode(mode)
          .setDescription(description).build()
      }
    }

    def toBqTypeNameWithComments(sparkType: DataType): StandardSQLTypeName = {

      sparkType match {
        case BooleanType => BOOL
        case ShortType | IntegerType | LongType => INT64
        case ByteType => BYTES
        case DoubleType => FLOAT64
        case DecimalType() => {
          val t = sparkType.asInstanceOf[DecimalType]
          val precision = t.precision
          val scale = t.scale
          if (scale > 9 || precision > 38) STRING else NUMERIC
        }
        case StringType | VarcharType(_) | CharType(_) => STRING
        case TimestampType => TIMESTAMP
        case DateType => DATE
        case _ => throw new Exception(s"$sparkType or Array[$sparkType] is not supported")
      }
    }

    Schema.of(sparkSchema.fields.map(sparkStructFieldToBigQueryField): _*)
  }
}
