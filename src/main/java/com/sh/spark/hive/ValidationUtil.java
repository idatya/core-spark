package com.sh.spark.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sh.spark.common.FileUtil;


public class ValidationUtil {
	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(ValidationHandler.class);
	static final String LOCATION = "LOCATION";
	static final String TABLE_PROPERTIES = "TABLE PROPERTIES";
	static final String SERDE_LIBRARY = "SERDE LIBRARY";
	static SparkSession session = null;

	private static SparkSession initializeSession() throws AnalysisException {
		session = SparkSession.builder().appName("TestApp")
				.config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
				.config("spark.master", "local")
				.config("deploy-mode", "cluster")
				.config("hive.metastore.uris", "thrift://192.168.218.63:9083")
				.config("spark.logConf", true)
				.config("spark.dynamicAllocation.enabled", true)
				.enableHiveSupport()
				.getOrCreate();
		
		session = SparkSession.builder().appName("sparkDataProcessor")
				.config("spark.logConf", true)
				.config("spark.dynamicAllocation.enabled", true)
				.config("spark.shuffle.service.enabled", true)				
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config("hive.exec.dynamic.partition", "true")
				.config("hive.exec.dynamic.partition.mode", "nonstrict")
				
				//.master("local[*]")
				.config("spark.master", "local[*]")
				.enableHiveSupport()
				.getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		
		
		return session;
	}

	public static void main(String[] args) throws AnalysisException {
		try {
			String hiveOutDB = args[0];
			String hiveOutTbl = args[1];
			String tdOutDB = args[2];
			String tdOutTbl = args[3];
			String skipColumnsStr = args[4];
			String validationQueryOutFile = "";
			String executionEngine = "spark";
			Boolean misMatchDataRequired = true;
			if (args.length == 6) {
				misMatchDataRequired = Boolean.parseBoolean(args[5]);
			}

			if (args.length == 7) {
				validationQueryOutFile = args[6];
				executionEngine = "hive";
			}
			initializeSession();
			Dataset<Row> hiveTblsDs = session.table(hiveOutDB + "." + hiveOutTbl);
			Dataset<Row> tdTblsDs = session.table(tdOutDB + "." + tdOutTbl);
			List<String> skipColumns = Arrays.asList(skipColumnsStr.toUpperCase().replaceAll("\\s+", "").split(","));
			StructType schema = hiveTblsDs.schema();
			StructField[] fields = schema.fields();

			StringBuffer srcConcatList = new StringBuffer();
			StringBuffer tgtConcatList = new StringBuffer();
			List<String> nullCheck = new ArrayList<String>();
			StringBuffer selectColumns = new StringBuffer();
			for (StructField structField : fields) {
				String columnName = structField.name();
				DataType columnDataType = structField.dataType();
				if (StringUtils.isNotEmpty(selectColumns.toString())) {
					selectColumns.append(",");
				}
				selectColumns.append("T." + columnName);
				generateMergeComponents(srcConcatList, tgtConcatList, skipColumns, columnName, columnDataType,
						nullCheck);
			}

			String srcContion = "CONCAT(" + srcConcatList.toString() + ")";
			String tgtContion = "CONCAT(" + tgtConcatList.toString() + ")";
			String valdiationQuery = "SELECT COUNT(*) from " + hiveOutDB + "." + hiveOutTbl + " AS T INNER JOIN "
					+ tdOutDB + "." + tdOutTbl + " AS S ON " + tgtContion + "=" + srcContion;

			String sampleDataMisMatchData = "SELECT " + selectColumns + " from " + hiveOutDB + "." + hiveOutTbl
					+ " AS T LEFT OUTER  JOIN " + tdOutDB + "." + tdOutTbl + " AS S ON " + tgtContion + "=" + srcContion
					+ " where S." + nullCheck.get(0) + " IS NULL  LIMIT 10";
			System.out.println("Query for getting Sample Data Mismatch:" + sampleDataMisMatchData);

			System.out.println("CELL BY CELL ValidiationQuery:\n" + valdiationQuery);
			if (executionEngine.equalsIgnoreCase("hive")) {

				String hiveProps = "\n SET hive.support.concurrency=true ; SET hive.enforce.bucketing=true ; SET hive.exec.dynamic.partition.mode=nonstrict ;"
						+ " SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager ; SET hive.exec.orc.split.strategy=BI ;\n--******************************";
				String tgtCntQ = "SELECT COUNT(*) from " + hiveOutDB + "." + hiveOutTbl + ";\n";
				String srcCntQ = "SELECT COUNT(*) from " + tdOutDB + "." + tdOutTbl + ";\n";
				String content = hiveProps + "\n" + tgtCntQ + srcCntQ + valdiationQuery + "\n --"
						+ sampleDataMisMatchData + ";\n--******************************";
				System.out.println(content);
				FileUtil.writeFileToPath(validationQueryOutFile + "/validation_" + hiveOutDB + "." + hiveOutTbl + "_"
						+ tdOutDB + "." + tdOutTbl + ".hive", content);

			} else {
				Dataset<Row> valdiationOutput = session.sql(valdiationQuery);
				System.out.println("******************************************");
				Long srcCount = tdTblsDs.count();
				System.out.println("Row  count for  SRC TD Table:" + tdOutDB + "." + tdOutTbl + ":" + srcCount);
				Long hiveCount = hiveTblsDs.count();
				System.out.println("Row  count for  TGT Hive Table:" + hiveOutDB + "." + hiveOutTbl + ":" + hiveCount);
				System.out.println("Count mismatch:" + (srcCount - hiveCount));
				Long cellByCellMatch = (Long) valdiationOutput.first().get(0);
				System.out.println("Cell By Cell Valdiation Count for SRC and TGT Tables:" + hiveOutDB + "."
						+ hiveOutTbl + "_" + tdOutDB + "." + tdOutTbl + ":" + cellByCellMatch);
				Long misMatch = srcCount - cellByCellMatch;

				System.out.println("Cell by Cell Mismatch with source:" + misMatch);
				if (misMatch != 0 && misMatchDataRequired) {
					Dataset<Row> sampleDataMisMatchDataDS = session.sql(sampleDataMisMatchData);

					System.out.println("########################################");

					sampleDataMisMatchDataDS.show(10, false);

					System.out.println("########################################");
				}

				System.out.println("******************************************");
			}

		} catch (Exception e) {
			//LOGGER.error("Error while running Spark convertor", e);
			e.printStackTrace();
		} finally {
			//session.stop();
		}
	}
	
	private static void generateMergeComponents(StringBuffer srcConcatList, StringBuffer tgtConcatList,
			List<String> skipColumns, String columnName, DataType columnDataType, List<String> nullCheck) {
		if (!skipColumns.contains(columnName.trim().toUpperCase())) {
			if (StringUtils.isNotEmpty(srcConcatList.toString())) {
				srcConcatList.append(",'IDM_WM',");
				tgtConcatList.append(",'IDM_WM',");

			}
			if (columnDataType == DataTypes.StringType) {
				srcConcatList.append(" UPPER(TRIM(nvl(S." + columnName + ",'IDW-WM')))");
				tgtConcatList.append(" UPPER(TRIM(nvl(T." + columnName + ",'IDW-WM')))");

			} else if (columnDataType == DataTypes.DateType) {

				srcConcatList.append(" nvl(S." + columnName + ",cast('1600-04-04'  as date))");
				tgtConcatList.append("nvl(T." + columnName + ",cast('1600-04-04'  as date))");

			} else if (columnDataType == DataTypes.TimestampType) {

				srcConcatList.append("nvl(S." + columnName + ",cast('1600-04-04 00:00:00' as timestamp))");
				tgtConcatList.append("nvl(T." + columnName + ",cast('1600-04-04 00:00:00' as timestamp))");

			} else {

				srcConcatList.append("nvl(S." + columnName + ",-99999)");
				tgtConcatList.append("nvl(T." + columnName + ",-99999)");

			}

			if (nullCheck.isEmpty()) {
				nullCheck.add(columnName);
			}

		}
	}

}
