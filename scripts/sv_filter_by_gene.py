import argparse
from argparse import RawTextHelpFormatter
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, coalesce, lit
import glow
import os

parser = argparse.ArgumentParser()
parser.add_argument('--annotated_sv_file', required=True)
parser.add_argument('--gene_list_file', required=True)
parser.add_argument('-S', '--field_separator', required=False, default='tab',
                    help='Field separator: options are "comma", "tab", or provide a single character like ";" or "|"')
parser.add_argument('--spark_executor_mem', type=int, default=4, help='Spark executor memory in GB')
parser.add_argument('--spark_executor_instance', type=int, default=1, help='Number of Spark executor instances')
parser.add_argument('--spark_executor_core', type=int, default=1, help='Number of Spark executor cores')
parser.add_argument('--spark_driver_maxResultSize', type=int, default=1, help='Spark driver max result size in GB')
parser.add_argument('--sql_broadcastTimeout', type=int, default=300, help='Spark SQL broadcast timeout in seconds')
parser.add_argument('--spark_driver_core', type=int, default=1, help='Number of Spark driver cores')
parser.add_argument('--spark_driver_mem', type=int, default=4, help='Spark driver memory in GB')
args = parser.parse_args()

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName('glow_pyspark') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes', 'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()
# Register so that glow functions like read vcf work with spark. Must be run in spark shell or in context described in help
spark = glow.register(spark)

# parameter configuration
gene_text_path = args.gene_list_file
annotated_sv_file = args.annotated_sv_file

# Determine actual separator character
separator_map = {
    'comma': ',',
    'tab': '\t',
    'semicolon': ';',
    'pipe': '|'
}
sep = separator_map.get(args.field_separator.lower(), args.field_separator)

# get a list of interested gene and remove unwanted strings in the end of each gene
gene_symbols_trunc = spark.read.option("header", False).text(gene_text_path)
gene_symbols_trunc = list(gene_symbols_trunc.toPandas()['value'])
gene_symbols_trunc = [gl.replace('\xa0', '').replace('\n', '') for gl in gene_symbols_trunc]

#main
sv_df = spark.read.options(inferSchema=True, sep=sep, header=True, nullValue="").csv(annotated_sv_file)

# Handle nulls and split Gene_name by semicolon, then explode to get one gene per row
sv_df_genes = sv_df.withColumn("Gene_name_split_col", explode(split(coalesce(col("Gene_name"), lit("")), ";")))

# name output file
base = os.path.basename(annotated_sv_file)
name, ext = os.path.splitext(base)
output_file = f"{name}.filtered{ext}"

# Trim gene names and filter by your gene list
sv_df_genes.withColumn("Gene_name_split_col", trim(col("Gene_name_split_col"))) \
                      .filter(col("Gene_name_split_col").isin(gene_symbols_trunc)) \
                      .toPandas() \
                        .to_csv(output_file, sep="\t", index=False, na_rep='-')