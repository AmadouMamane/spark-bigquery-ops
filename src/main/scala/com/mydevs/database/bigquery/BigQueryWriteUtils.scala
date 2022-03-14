package com.mydevs.database.bigquery

import com.google.cloud.bigquery.StandardTableDefinition.StreamingBuffer
import com.google.cloud.bigquery.{EncryptionConfiguration, SnapshotTableDefinition, TableDefinition}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.joda.time.{DateTimeZone, LocalDate, LocalDateTime}
import org.slf4j.LoggerFactory

/**
 * Utilities for writing into big query using spark bigquery connector
 */
object BigQueryWriteUtils {

  val logger = LoggerFactory.getLogger("BigQueryWriteUtils")

  object BQColumnComment extends Enumeration {
    type BQColumnComment = Value
    val BigDecimalType, TimestampType, StringType = Value
  }

  // Writing big query table
  def writeIntoBigQuery(sparkDataframe: Dataset[_],
                        bqTableName: String,
                        bqDatasetName: String,
                        archiveTable: Boolean,
                        createBqTableIfNotExists: Boolean,
                        projectId: String,
                        // Value for creating google cloud storage path when archiving is specified.
                        // This is most commonly the value of the country_code column
                        partitioningFieldValue: Option[Any] = None,
                        writeMode: String = "owerwrite",
                        // Metada for specifying Big Query table partitions
                        partitioningMetadata: Option[BqPartition] = None,
                        clusteringFields: Option[Seq[String]] = None,
                        additionalWriteOptions: Option[Map[String, String]] = None,
                        temporaryGcsBucketName: Option[String] = temporaryGcsBucketName,
                        archiverGcsBucketName: Option[String] = archiverGcsBucketName,
                        manageBigDecimalTypes: Option[Boolean] = Some(false),
                        bigDecimalTypesIndicesInDataset: Option[Seq[Int]] = None,
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
                        expirationTime: Option[Long] = None)(implicit spark: SparkSession) = {

    val zone = DateTimeZone.forID("Europe/Paris")

    val timestampPartition = LocalDateTime.now(zone).toString("yyyyMMdd_HHmmss")

    val yearPartition = LocalDate.now(zone).year.get

    val commonWriteOptions = collection.mutable.Map("intermediateFormat" -> "orc", "parentProject" -> projectId)

    val writeOptions = archiveTable match {
      case true =>
        require(archiverGcsBucketName.isDefined, "The archiverGcsBucketName has not been defined")
        val archiverGcsPath = if (partitioningMetadata.isDefined) {
          require(partitioningFieldValue.isDefined, "Partitioning column value is mendatory for archiving a partitioned table")
          val partitioningColumnNamesStr = s"${partitioningMetadata.get.partitioningColumnName}=${partitioningFieldValue.get.toString}"
          s"ar_$bqTableName/$yearPartition/$partitioningColumnNamesStr/$timestampPartition"
        } else {
          require(temporaryGcsBucketName.isDefined, "The temporaryGcsBucketName has not been defined")
          s"ar_$bqTableName/$yearPartition/$timestampPartition"
        }
        commonWriteOptions ++ Map("writeMethod" -> "indirect", "persistentGcsBucket" -> archiverGcsBucketName.get, "persistentGcsPath" -> archiverGcsPath)
      case _ => commonWriteOptions ++ Map("writeMethod" -> "direct", "temporaryGcsBucket" -> s"${temporaryGcsBucketName.get}/bq_connector_tmp")
    }

    if (clusteringFields.isDefined) writeOptions ++ Map("clusteringFields" -> clusteringFields.get.mkString(","))

    if (partitioningMetadata.isDefined) {
      val metadata = partitioningMetadata.get
      writeOptions ++ Map("partitionField" -> metadata.partitioningColumnName, "partitionType" -> metadata.partitioningColumnType)
    }

    if (additionalWriteOptions.isDefined) writeOptions ++ additionalWriteOptions.get

    val dataFrameToWrite = if (manageBigDecimalTypes.isDefined && !manageBigDecimalTypes.get) sparkDataframe else {
      import BqWriteCastUtils._
      val actualBigDecimalTypesIndicesInDataset = if (bigDecimalTypesIndicesInDataset.isDefined) bigDecimalTypesIndicesInDataset else
        Option(sparkDataframe.schema.zipWithIndex.map(x => if (doesStructFieldContainsBigDecimalType(x._1)) Some(x._2) else None).filter(_.isDefined).map(_.get))
      transformBigDecimalTypesToStringTypesAndMakeComment(sparkDataframe, spark, actualBigDecimalTypesIndicesInDataset)
    }

    if (createBqTableIfNotExists) createTable(bqDatasetName, bqTableName, projectId, Some(dataFrameToWrite.schema),
      partitioningMetadata, clusteringFields, requirePartitionFilter, friendlyName,
      description, location, labels, tableType, materializedView, columnsMetadata, view, externalDataConfiguration, encryptionConfiguration, streamingBuffer, snapshotTableDefinition, numBytes, numLongTermBytes, numRows, expirationTime)

    logger.info("sparkDataframe schema" + sparkDataframe.schema.treeString)
    logger.info("dataFrameToWrite schema" + dataFrameToWrite.schema.treeString)

    if (!externalDataConfiguration.isDefined) {

      val fullPartitionedTableName = if (partitioningMetadata.isDefined) {
        s"$projectId.$bqDatasetName.$bqTableName" + "$" + partitioningFieldValue.get.toString
      } else {
        s"$projectId.$bqDatasetName.$bqTableName"
      }

      dataFrameToWrite.write.format("bigquery").option("table", fullPartitionedTableName).options(writeOptions).mode(writeMode).save()

      //Updating table schema is needing in case of overwrite and the user specified some table properties in the create table options
      if (createBqTableIfNotExists && manageBigDecimalTypes.isDefined && manageBigDecimalTypes.get) {
        updateTableSchema(bqDatasetName, bqTableName, projectId, Some(dataFrameToWrite.schema), partitioningMetadata,
          clusteringFields, requirePartitionFilter, friendlyName,
          description, location, labels, tableType, materializedView, columnsMetadata, view, externalDataConfiguration, encryptionConfiguration, streamingBuffer, snapshotTableDefinition, numBytes, numLongTermBytes, numRows, expirationTime)
      }

    } else {
      logger.warn(s"Loading data in table $projectId:$bqDatasetName.$bqTableName is not allowed for this operation because it is currently an EXTERNAL table")
    }

  }

  object BqWriteCastUtils {

    def doesStructFieldContainsBigDecimalType(in: StructField): Boolean = {
      def isDataTypeBigDecimalType(in: DataType): Boolean = {
        in match {
          case BooleanType | ShortType | IntegerType | LongType | DoubleType | ByteType | TimestampType | DateType | StringType => false
          case DecimalType() => {
            val d = in.asInstanceOf[DecimalType]
            if (d.scale > 9 || d.precision > 38) true else false
          }
          case s => throw new Exception(s"$s is not yet supported")
        }
      }

      val dt = in.dataType
      dt match {
        case ArrayType(e, _) => {
          e match {
            case ArrayType(_, _) => throw new Exception("Array of array are not supported")
            case BinaryType => false
            case StructType(fields) =>
              val r = Option(fields.map(doesStructFieldContainsBigDecimalType).filter(_ == true))
              if (r.isDefined) r.get.head else false
            case _ => isDataTypeBigDecimalType(e)
          }
        }
        case StructType(fields) =>
          val r = Option(fields.map(doesStructFieldContainsBigDecimalType).filter(_ == true))
          if (r.isDefined) r.get.head else false
        case _ => isDataTypeBigDecimalType((dt))
      }
    }

    def transformBigDecimalTypesToStringTypesAndMakeComment(inputDataset: Dataset[_], spark: SparkSession, inputBigDecimalTypesIndicesInDataset: Option[Seq[Int]] = None) = {
      val inputDsSchema = inputDataset.schema
      val inputDsSchemaFields = inputDsSchema.fields

      val bigDecimalTypesIndicesInDataset = if (!inputBigDecimalTypesIndicesInDataset.isDefined) {
        val bigDecimalTypesIndicesInDatasetZip = inputDsSchema.map(f => f.dataType.simpleString).map(_.contains("decimal(38,18)")).zipWithIndex
        bigDecimalTypesIndicesInDatasetZip.filter(_._1 == true).map(_._2)
      } else {
        inputBigDecimalTypesIndicesInDataset.get
      }
      val bigDecimalFieldsInDataset = bigDecimalTypesIndicesInDataset.map(inputDsSchemaFields(_))
      val bigDecimalDataTypesInDataset = bigDecimalFieldsInDataset.map(_.dataType)
      val remainingColumnsIndicesInDataset = inputDsSchemaFields.indices.diff(bigDecimalTypesIndicesInDataset)
      val remainingFieldsInDataset = remainingColumnsIndicesInDataset.map(inputDsSchemaFields.lift).flatten

      val transformedRdd = inputDataset.toDF.rdd.map(row => {
        val inputRowColumns = row.toSeq
        val columnsToTransformToString = bigDecimalTypesIndicesInDataset.map(inputRowColumns(_))
        val remainingColumns = remainingColumnsIndicesInDataset.map(inputRowColumns.lift).flatten
        val transformedToString = columnsToTransformToString.zip(bigDecimalDataTypesInDataset).map(bigDecCols => {
          //dataType = bigDecCols._2
          val dataTypeToScalaType = sparkTypeToScalaType(bigDecCols._2)
          //dataValue = bigDecCols._1
          createStringValue(bigDecCols._1).asInstanceOf[dataTypeToScalaType.type]
        })
        Row(transformedToString ++ remainingColumns: _*)
      })

      val transformedBigDecimalColsNewSchema = bigDecimalFieldsInDataset.map(transformBigDecimalFieldToStringField).toArray
      val transformedSchema = new StructType(transformedBigDecimalColsNewSchema ++ remainingFieldsInDataset)

      spark.createDataFrame(transformedRdd, transformedSchema).selectExpr(inputDataset.columns: _*)
    }

    def mkComment(sparkComment: Option[String], bqComment: Option[BQColumnComment.BQColumnComment] = Some(BQColumnComment.BigDecimalType)) = Seq(bqComment, sparkComment).filter(c => c.isDefined && c.get != "").map(_.get).mkString("|")

    def sparkTypeToScalaType(in: DataType): AnyRef = in match {
      case IntegerType => Integer.TYPE
      case DecimalType() => {
        val t = in.asInstanceOf[DecimalType]
        val precision = t.precision
        val scale = t.scale
        if (scale > 9 || precision > 38) StringType else in
      }
      case StringType => StringType
      case MapType(k, v, _) =>
        val d = sparkTypeToScalaType(k)
        val n = sparkTypeToScalaType(v)
        Map[d.type, n.type]()
      case ArrayType(e, _) =>
        val z = sparkTypeToScalaType(e)
        Seq[z.type]()
      case StructType(fields) => fields.map(f => sparkTypeToScalaType(f.dataType))
      case s => throw new Exception(s"$s is not yet supported")
    }

    def createStringValue(in: Any): Any = {
      if (in == null) in else {
        in match {
          case i: Boolean => i
          case i: Short => i
          case i: Int => i
          case i: Long => i
          case i: Double => i
          case i: Byte => i
          case i: Char => i
          case i: String => i
          case i: JBigDecimal => {
            val precision = i.precision
            val scale = i.scale
            if (scale > 9 || precision > 38) i.toString else i
          }
          case i: Map[_, _] => i.asInstanceOf[Map[_, _]].map(x => createStringValue(x._1) -> createStringValue(x._2))
          case i: Seq[_] => i.map(createStringValue)
          case i: Row => Row(i.toSeq.map(createStringValue): _*)
          case i: Any => throw new Exception(s"${i.getClass} of $i is not supported")
        }
      }
    }

    def transformBigDecimalTypesToStringTypes(sparkType: DataType): (DataType, Option[BQColumnComment.BQColumnComment]) = {
      sparkType match {
        case BooleanType | ShortType | IntegerType | LongType | DoubleType | ByteType | StringType | VarcharType(_) | CharType(_) | TimestampType | DateType => (sparkType, None)
        case DecimalType() => {
          val t = sparkType.asInstanceOf[DecimalType]
          val precision = t.precision
          val scale = t.scale
          if (scale > 9 || precision > 38) (StringType, Some(BQColumnComment.BigDecimalType)) else (sparkType, None)
        }
        case _ => throw new Exception(s"$sparkType or Array[$sparkType] is not supported")
      }
    }

    def transformBigDecimalFieldToStringField(sparkField: StructField): StructField = {
      val name = sparkField.name
      val dtType = sparkField.dataType
      val nullable = sparkField.nullable
      val metadata = sparkField.metadata
      val comment = sparkField.getComment().getOrElse("")
      dtType match {
        case DecimalType() => {
          val t = dtType.asInstanceOf[DecimalType]
          val precision = t.precision
          val scale = t.scale
          if (scale > 9 || precision > 38) {
            StructField(name, StringType, nullable, metadata).withComment(mkComment(Some(comment)))
          } else sparkField
        }
        case ArrayType(elementType, _) => elementType match {
          case ArrayType(_, _) => throw new Exception("Array of array are not supported in BigQuery")
          case BinaryType => StructField(name, ArrayType(elementType), nullable, metadata).withComment(mkComment(Some(comment)))
          case StructType(fields) => StructField(name, ArrayType(StructType.apply(fields.map(transformBigDecimalFieldToStringField))), nullable, metadata).withComment(mkComment(Some(comment)))
          case _ => StructField(name, ArrayType(transformBigDecimalTypesToStringTypes(elementType)._1), nullable, metadata).withComment(mkComment(Some(comment)))
        }
        case StructType(fields) => StructField(name, StructType.apply(fields.map(transformBigDecimalFieldToStringField)), nullable, metadata).withComment(comment)
        case _ => StructField(name, transformBigDecimalTypesToStringTypes(dtType)._1, nullable, metadata).withComment(comment)
      }
    }
  }

}
