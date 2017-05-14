/**
 * DataCleaner (community edition)
 * Copyright (C) 2014 Neopost - Customer Information Management
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.datacleaner.spark;

import static org.apache.metamodel.csv.CsvConfiguration.DEFAULT_COLUMN_NAME_LINE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.fixedwidth.FixedWidthConfiguration;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.Resource;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.datacleaner.api.AnalyzerResult;
import org.datacleaner.api.InputRow;
import org.datacleaner.connection.CsvDatastore;
import org.datacleaner.connection.Datastore;
import org.datacleaner.connection.DatastoreConnection;
import org.datacleaner.connection.FixedWidthDatastore;
import org.datacleaner.connection.JdbcDatastore;
import org.datacleaner.connection.JsonDatastore;
import org.datacleaner.job.AnalysisJob;
import org.datacleaner.job.runner.AnalysisResultFuture;
import org.datacleaner.job.runner.AnalysisRunner;
import org.datacleaner.spark.functions.AnalyzerResultReduceFunction;
import org.datacleaner.spark.functions.ExtractAnalyzerResultFunction;
import org.datacleaner.spark.functions.FixedWidthParserFunction;
import org.datacleaner.spark.functions.RowProcessingFunction;
import org.datacleaner.spark.functions.TuplesToTuplesFunction;
import org.datacleaner.spark.functions.ValuesToInputRowFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class SparkAnalysisRunner implements AnalysisRunner {

    private static final Logger logger = LoggerFactory.getLogger(SparkAnalysisRunner.class);

    private final SparkJobContext _sparkJobContext;
    private final SparkSession _sparkSession;

    public SparkAnalysisRunner(final JavaSparkContext sparkContext, final SparkJobContext sparkJobContext) {
        _sparkJobContext = sparkJobContext;
        _sparkSession = SparkSession.builder().config(sparkContext.getConf()).getOrCreate();
    }

    @Override
    public AnalysisResultFuture run(final AnalysisJob job) {
        return run();
    }

    public AnalysisResultFuture run() {
        _sparkJobContext.triggerOnJobStart();
        final AnalysisJob analysisJob = _sparkJobContext.getAnalysisJob();
        final Datastore datastore = analysisJob.getDatastore();

        final Table table = analysisJob.getSourceColumns().get(0).getPhysicalColumn().getTable();

        final JavaRDD<InputRow> inputRowsRDD = openSourceDatastore(datastore, table);

        final JavaPairRDD<String, NamedAnalyzerResult> namedAnalyzerResultsRDD;
        if (_sparkJobContext.getAnalysisJobBuilder().isDistributable()) {
            logger.info("Running the job in distributed mode");

            // TODO: We have yet to get more experience with this setting - do a
            // benchmark of what works best, true or false.
            final boolean preservePartitions = true;

            final JavaRDD<Tuple2<String, NamedAnalyzerResult>> processedTuplesRdd = inputRowsRDD
                    .mapPartitionsWithIndex(new RowProcessingFunction(_sparkJobContext), preservePartitions);

            if (_sparkJobContext.isResultEnabled()) {
                final JavaPairRDD<String, NamedAnalyzerResult> partialNamedAnalyzerResultsRDD =
                        processedTuplesRdd.mapPartitionsToPair(new TuplesToTuplesFunction<>(), preservePartitions);

                namedAnalyzerResultsRDD =
                        partialNamedAnalyzerResultsRDD.reduceByKey(new AnalyzerResultReduceFunction(_sparkJobContext));
            } else {
                // call count() to block and wait for RDD to be fully processed
                processedTuplesRdd.count();
                namedAnalyzerResultsRDD = null;
            }
        } else {
            logger.warn("Running the job in non-distributed mode");
            final JavaRDD<InputRow> coalescedInputRowsRDD = inputRowsRDD.coalesce(1);
            namedAnalyzerResultsRDD =
                    coalescedInputRowsRDD.mapPartitionsToPair(new RowProcessingFunction(_sparkJobContext));

            if (!_sparkJobContext.isResultEnabled()) {
                // call count() to block and wait for RDD to be fully processed
                namedAnalyzerResultsRDD.count();
            }
        }

        if (!_sparkJobContext.isResultEnabled()) {
            final List<Tuple2<String, AnalyzerResult>> results = Collections.emptyList();
            return new SparkAnalysisResultFuture(results, _sparkJobContext);
        }

        assert namedAnalyzerResultsRDD != null;
        final JavaPairRDD<String, AnalyzerResult> finalAnalyzerResultsRDD =
                namedAnalyzerResultsRDD.mapValues(new ExtractAnalyzerResultFunction());

        // log analyzer results
        final List<Tuple2<String, AnalyzerResult>> results = finalAnalyzerResultsRDD.collect();

        logger.info("Finished! Number of AnalyzerResult objects: {}", results.size());
        for (final Tuple2<String, AnalyzerResult> analyzerResultTuple : results) {
            final String key = analyzerResultTuple._1;
            final AnalyzerResult result = analyzerResultTuple._2;
            logger.info("AnalyzerResult (" + key + "):\n\n" + result + "\n");
        }

        _sparkJobContext.triggerOnJobEnd();
        return new SparkAnalysisResultFuture(results, _sparkJobContext);
    }

    private JavaRDD<InputRow> openSourceDatastore(final Datastore datastore, final Table table) {
        if (datastore instanceof CsvDatastore) {
            final CsvDatastore csvDatastore = (CsvDatastore) datastore;
            final Resource resource = csvDatastore.getResource();
            assert resource != null;
            final String datastorePath = resource.getQualifiedPath();

            final CsvConfiguration csvConfiguration = csvDatastore.getCsvConfiguration();

            if (csvConfiguration.getColumnNameLineNumber() != DEFAULT_COLUMN_NAME_LINE) {
                throw new IllegalStateException("Only default header line allowed");
            }


            final DataFrameReader read =
                    _sparkSession.read().option("quote", String.valueOf(csvConfiguration.getQuoteChar()))
                            .option("escape", String.valueOf(csvConfiguration.getEscapeChar()))
                            .option("sep", String.valueOf(csvConfiguration.getSeparatorChar()))
                            .option("encoding", csvConfiguration.getEncoding()).option("header", true);


            final JavaPairRDD<Row, Long> zipWithIndex = read.csv(datastorePath).javaRDD().zipWithIndex();

            return zipWithIndex.map(new ValuesToInputRowFunction(_sparkJobContext));
        } else if (datastore instanceof JsonDatastore) {

            final JsonDatastore jsonDatastore = (JsonDatastore) datastore;

            final String datastorePath = jsonDatastore.getResource().getQualifiedPath();
            final Dataset<Row> rawInput =
                    _sparkSession.read().schema(getSparkSchema(jsonDatastore)).json(datastorePath);

            return rawInput.javaRDD().zipWithIndex().map(new ValuesToInputRowFunction(_sparkJobContext));
        } else if (datastore instanceof FixedWidthDatastore) {
            final FixedWidthDatastore fixedWidthDatastore = (FixedWidthDatastore) datastore;

            final FixedWidthConfiguration fixedWidthConfiguration = fixedWidthDatastore.getConfiguration();

            final String datastorePath = fixedWidthDatastore.getResource().getQualifiedPath();

            final Dataset<String> rawInput = _sparkSession.read().textFile(datastorePath);

            final JavaRDD<String[]> parsedInput =
                    rawInput.javaRDD().map(new FixedWidthParserFunction(fixedWidthConfiguration));

            JavaPairRDD<Row, Long> zipWithIndex =
                    parsedInput.map((Function<String[], Row>) record -> RowFactory.create((Object[]) record))
                            .zipWithIndex();

            if (fixedWidthConfiguration.getColumnNameLineNumber() != FixedWidthConfiguration.NO_COLUMN_NAME_LINE) {
                zipWithIndex = zipWithIndex.filter(new SkipHeaderLineFunction(fixedWidthConfiguration
                        .getColumnNameLineNumber()));
            }

            return zipWithIndex.map(new ValuesToInputRowFunction(_sparkJobContext));
        } else if (datastore instanceof JdbcDatastore) {
            final JdbcDatastore jdbcDatastore = (JdbcDatastore) datastore;
            String productName = "";
            try {
                productName = jdbcDatastore.createConnection().getMetaData().getDatabaseProductName();
            } catch (SQLException e) {
                logger.warn("Could not read product name", e);
                // Let's try to continue using normal MetaModel JDBC (It will probably fail, though)
            }

            final Dataset<Row> rawInput;
            if (productName.equals(JdbcDataContext.DATABASE_PRODUCT_HIVE)) {
                rawInput = _sparkSession.sql("SELECT * FROM " + table.getName());
            } else {
                final Properties properties = new Properties();
                properties.setProperty("username", jdbcDatastore.getUsername());
                properties.setProperty("password", jdbcDatastore.getPassword());

                rawInput = _sparkSession.read().jdbc(jdbcDatastore.getJdbcUrl(), table.getName(), properties);
            }

            return rawInput.javaRDD().zipWithIndex().map(new ValuesToInputRowFunction(_sparkJobContext));
        }

        throw new UnsupportedOperationException("Unsupported datastore type or configuration: " + datastore);
    }

    private StructType getSparkSchema(final Datastore datastore) {
        final StructType schema;
        try (DatastoreConnection openConnection = datastore.openConnection()) {
            final Column[] columns = openConnection.getDataContext().getDefaultSchema().getTable(0).getColumns();
            final List<StructField> fields = new ArrayList<>();
            for (Column column : columns) {
                fields.add(DataTypes.createStructField(column.getName(), DataTypes.StringType, true));
            }
            schema = DataTypes.createStructType(fields);
        }
        return schema;
    }
}
