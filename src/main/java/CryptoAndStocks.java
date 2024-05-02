import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class CryptoAndStocks {

    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("crypto-stocks")
                .master("local[*]")
                .config("mapreduce.fileoutputcomitter.marksuccessfuljobs","false")
                .getOrCreate();
    }

    private static Dataset<Row> getDataset(SparkSession spark, String path, String format) {

        Dataset<Row> dataset = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(path);

        return enrichDataset(dataset, format);
    }

    private static Dataset<Row> enrichDataset(Dataset<Row> dataset, String format){
        Dataset<Row> rowDataset = dataset.select("date", "high", "low");
        return rowDataset
                .withColumn("Year", year(to_date(rowDataset.col("date"),format)))
                .withColumn("high_Casted", regexp_replace(rowDataset.col("high"),",","").cast(DataTypes.FloatType))
                .withColumn("low_Casted", regexp_replace(rowDataset.col("low"),",","").cast(DataTypes.FloatType))
                .withColumn("date", to_date(rowDataset.col("date"), format))
                .drop("low", "high")
                .withColumnRenamed("low_Casted", "low")
                .withColumnRenamed("high_Casted", "high");

        }

     private static Dataset<Row> renameColumn (Dataset<Row> dataset, String identifier){
        return dataset
                .withColumnRenamed("high",identifier+"_high")
                .withColumnRenamed("low",identifier+"_low")
                .where("year > 2017")
                .where("year < 2024");
        }

        private static Dataset<Row> sqlQuery(SparkSession spark){
            Dataset<Row> joinDataset= spark.sql("SELECT NVL(A.DATE, NVL(B.DATE, E.DATE)) DATE, NVL(A.YEAR, NVL(B.YEAR, E.YEAR)) YEAR, " +
                    " APPLE_HIGH, APPLE_LOW, ETHEREUM_HIGH, ETHEREUM_LOW, BITCOIN_HIGH, BITCOIN_LOW FROM APPLE A FULL OUTER JOIN BITCOIN B ON A.DATE == B.DATE " +
                    " FULL OUTER JOIN ETHEREUM E ON A.DATE == E.DATE ORDER BY DATE ASC ");

            joinDataset.createOrReplaceTempView("merge");

            Dataset<Row> finalJoinDataset = spark.sql("SELECT DATE, YEAR, SUM(APPLE_HIGH)," +
                    "SUM(APPLE_LOW), SUM(ETHEREUM_HIGH), SUM(ETHEREUM_LOW), " +
                    "SUM(BITCOIN_HIGH), SUM(BITCOIN_LOW) " +
                    "FROM MERGE GROUP BY DATE, YEAR ORDER BY DATE, YEAR ASC")
                    .withColumnRenamed("sum(ETHEREUM_HIGH)", "ethereum_high")
                    .withColumnRenamed("sum(ETHEREUM LOW)", "ethereum_low")
                    .withColumnRenamed("sum(BITCOIN_HIGH)", "bitcoin_high")
                    .withColumnRenamed("sum(BITCOIN_LOW)", "bitcoin_low")
                    .withColumnRenamed("sum(APPLE_HIGH)", "apple_high")
                    .withColumnRenamed("sum(APPLE_LOW)", "apple_low");

            return finalJoinDataset;

        }

    public static void main(String[] args){

        String appleDateFormat="yyyy-MM-dd";
        String cryptoDateFormat="dd/MM/yyyy";

        String etheriumPath="src/main/resources/Ethereum Historical Data2.csv";
        String bitcoinPath="src/main/resources/Bitcoin Historical Data2.csv";
        String applePath="src/main/resources/aapl_2014_2023.csv";
        String finalOutputPath="src/main/output";

        SparkSession spark = getSparkSession();

        Dataset<Row> etheriumDataset= getDataset(spark,etheriumPath,cryptoDateFormat);
        Dataset<Row> bitcoinDataset= getDataset(spark, bitcoinPath, cryptoDateFormat);
        Dataset<Row> appleDataset= getDataset(spark, applePath,appleDateFormat);

        Dataset<Row> etherDataset= renameColumn(etheriumDataset,"ethereum");
        Dataset<Row> bitDataset= renameColumn(bitcoinDataset,"bitcoin");
        Dataset<Row> appDataset= renameColumn(appleDataset,"apple");

        //For Sql query
        etherDataset.createOrReplaceTempView("ethereum");
        bitDataset.createOrReplaceTempView("bitcoin");
        appDataset.createOrReplaceTempView("apple");

        Dataset<Row> sqlOutput= sqlQuery(spark);

        Dataset<Row> finalDatset = sqlOutput
                .withColumn("apple_20day_avg", avg (sqlOutput.col("apple_high")).over(Window.orderBy(sqlOutput.col("date").asc()).rowsBetween(-19, 0)))
                .withColumn("bitcoin_20day_avg", avg(sqlOutput.col( "bitcoin_high")).over(Window.orderBy(sqlOutput.col("date").asc()).rowsBetween (-19, 8)))
                .withColumn("ethereum_20day_avg", avg(sqlOutput.col("ethereum_high")).over(Window.orderBy(sqlOutput.col("date").asc()).rowsBetween(-19, 0)));

        finalDatset.show();

        finalDatset
                .coalesce(1)
                .write()
                .option("header", true)
                .mode("overwrite")
                .csv(finalOutputPath);

    }

}

