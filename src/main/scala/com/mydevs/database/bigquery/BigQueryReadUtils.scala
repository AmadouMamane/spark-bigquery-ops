package com.mydevs.database.bigquery

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * Utilities for reading big query tables using spark bigquery connector
 */
object BigQueryReadUtils {

  import BqReadCastUtils._

  // Reading big query table
  def readBigQueryTable(bqTableName: String,
                        bqDatasetName: String,
                        projectId: String,
                        manageBigDecimalTypes: Option[Boolean] = Some(true),
                        bigDecimalTypesIndicesInDataset: Option[Seq[Int]] = None)(implicit spark: SparkSession) = {
    val bqDataframe = spark.read.format("bigquery").load(s"$projectId.$bqDatasetName.$bqTableName")

    val transformedBqDataframe = if (manageBigDecimalTypes.isDefined && !manageBigDecimalTypes.get) {
      bqDataframe
    } else {
      val bqDfSchema = bqDataframe.schema
      val actualBigDecimalTypesIndicesInDataset = if (bigDecimalTypesIndicesInDataset.isDefined) bigDecimalTypesIndicesInDataset else
        Option(bqDfSchema.zipWithIndex.map(x => if (getCommentFromStructField(x._1)) Some(x._2) else None).filter(_.isDefined).map(_.get))
      transformCommentedBqStringsToBigDecimalTypes(bqDataframe, spark, actualBigDecimalTypesIndicesInDataset)
    }
    transformedBqDataframe.show(false)
    transformedBqDataframe.printSchema()
  }

  def transformCommentedBqStringsToBigDecimalTypes(inputDataset: Dataset[_], spark: SparkSession, inputBigDecimalTypesIndicesInDataset: Option[Seq[Int]] = None) = {
    val inputDsSchema = inputDataset.schema
    val inputDsSchemaFields = inputDsSchema.fields

    val bigDecimalTypesIndicesInDataset = if (!inputBigDecimalTypesIndicesInDataset.isDefined) {
      val bigDecimalTypesIndicesInDatasetZip = inputDsSchema.map(f => f.dataType.simpleString).map(_.contains("decimal(38,18)")).zipWithIndex
      bigDecimalTypesIndicesInDatasetZip.filter(_._1 == true).map(_._2)
    } else {
      inputBigDecimalTypesIndicesInDataset.get
    }
    val bigDecimalFieldsInDataset = bigDecimalTypesIndicesInDataset.map(inputDsSchemaFields(_))
    val remainingColumnsIndicesInDataset = inputDsSchemaFields.indices.diff(bigDecimalTypesIndicesInDataset)
    val remainingFieldsInDataset = remainingColumnsIndicesInDataset.map(inputDsSchemaFields.lift).flatten

    val transformedRdd = inputDataset.toDF.rdd.map(row => {
      val inputRowColumns = row.toSeq
      val columnsToTransformToString = bigDecimalTypesIndicesInDataset.map(inputRowColumns(_))
      val remainingColumns = remainingColumnsIndicesInDataset.map(inputRowColumns.lift).flatten
      val transformedToString = columnsToTransformToString.zip(bigDecimalFieldsInDataset).map(bigDecCols => {
        val dataField = bigDecCols._2
        val dataTypeToScalaType = sparkTypeToScalaType(dataField.dataType, dataField.getComment())
        val dataValue = bigDecCols._1
        createBigDecimalValue(dataValue, dataField).asInstanceOf[dataTypeToScalaType.type]
      })
      Row(transformedToString ++ remainingColumns: _*)
    })

    val transformedBigDecimalColsNewSchema = bigDecimalFieldsInDataset.map(transformBigDecimalFieldToStringField).toArray
    val transformedSchema = new StructType(transformedBigDecimalColsNewSchema ++ remainingFieldsInDataset)

    spark.createDataFrame(transformedRdd, transformedSchema).selectExpr(inputDataset.columns: _*)
  }

  object BqReadCastUtils {
    def getCommentFromStructField(in: StructField): Boolean = {
      val cm = in.getComment()
      val dt = in.dataType
      dt match {
        case ArrayType(e, _) => {
          e match {
            case ArrayType(_, _) => throw new Exception("Array of array are not supported")
            case BinaryType => false
            case StructType(fields) =>
              val r = Option(fields.map(getCommentFromStructField).filter(_ == true))
              if (r.isDefined) r.get.head else false
            case _ => getCommentFromDataType(e, cm)
          }

        }
        case StructType(fields) =>
          val r = Option(fields.map(getCommentFromStructField).filter(_ == true))
          if (r.isDefined) r.get.head else false
        case _ => getCommentFromDataType((dt, cm))
      }
    }

    def getCommentFromDataType(in: (DataType, Option[String])): Boolean = {
      val cm = in._2
      val isDec = cm.isDefined && cm.get.contains(BQColumnComment.BigDecimalType.toString)
      val dt = in._1
      dt match {
        case BooleanType | ShortType | IntegerType | LongType | DoubleType | ByteType | TimestampType | DateType | DecimalType() => false
        case StringType => isDec
        case s => throw new Exception(s"$s is not yet supported")
      }
    }

    def sparkTypeToScalaType(in: DataType, cm: Option[String]): AnyRef = in match {
      case BooleanType | ShortType | IntegerType | LongType | DoubleType | ByteType | TimestampType | DateType | DecimalType() => in

      case StringType => {
        val isDec = cm.isDefined && cm.get.contains(BQColumnComment.BigDecimalType.toString)
        if (isDec) {
          DecimalType(38, 18)
        } else StringType
      }
      case ArrayType(e, _) =>
        val z = sparkTypeToScalaType(e, cm)
        Seq[z.type]()
      case StructType(fields) => fields.map(f => sparkTypeToScalaType(f.dataType, f.getComment()))
      case s => throw new Exception(s"$s is not yet supported")
    }

    def createBigDecimalValue(in: Any, cm: StructField): Any = {
      if (in == null) in else {
        in match {
          case i: Boolean => i
          case i: Short => i
          case i: Int => i
          case i: Long => i
          case i: Double => i
          case i: Byte => i
          case i: Char => i
          case i: JBigDecimal => i
          case i: String => {
            val comment = cm.getComment()
            val isDec = comment.isDefined && comment.get.contains(BQColumnComment.BigDecimalType.toString)
            if (isDec) new JBigDecimal(i) else i
          }
          case i: Seq[_] => i.map(x => createBigDecimalValue(x, cm))
          case i: Row => {
            val dtType = in.asInstanceOf[GenericRowWithSchema].schema
            Row(i.toSeq.zipWithIndex.map(x => createBigDecimalValue(x._1, dtType(x._2))): _*)
          }
          case i: Any => throw new Exception(s"${i.getClass} of $i is not supported")
        }
      }
    }

    def transformCommentedStringTypesToBigDecimalTypes(sparkType: DataType, cm: Option[String]): DataType = {
      sparkType match {
        case BooleanType | ShortType | IntegerType | LongType | DoubleType | ByteType | CharType(_) | TimestampType | DateType | DecimalType() => sparkType
        case StringType => {
          val isDec = cm.isDefined && cm.get.contains(BQColumnComment.BigDecimalType.toString)
          if (isDec) {
            DecimalType(38, 18)
          } else StringType
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
        case ArrayType(elementType, _) => elementType match {
          case ArrayType(_, _) => throw new Exception("Array of array are not supported in BigQuery")
          case BinaryType => sparkField
          case StructType(fields) => StructField(name, ArrayType(StructType.apply(fields.map(transformBigDecimalFieldToStringField))), nullable, metadata).withComment(comment)
          case _ => StructField(name, ArrayType(transformCommentedStringTypesToBigDecimalTypes(elementType, Some(comment))), nullable, metadata).withComment(comment)
        }
        case StructType(fields) => StructField(name, StructType.apply(fields.map(transformBigDecimalFieldToStringField)), nullable, metadata).withComment(comment)
        case _ => StructField(name, transformCommentedStringTypesToBigDecimalTypes(dtType, Some(comment)), nullable, metadata).withComment(comment)
      }
    }
  }

}
