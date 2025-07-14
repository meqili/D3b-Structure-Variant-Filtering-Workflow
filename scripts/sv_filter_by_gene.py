import sys
from argparse import RawTextHelpFormatter, ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, trim, coalesce, lit, regexp_extract
import glow
import os
from functools import reduce

# 1. Argument parsing with filtering flags
parser = ArgumentParser(
    description="""This script is to filter SVs by column Gene_name or RE_gene
based on the genes interested""",
    formatter_class=RawTextHelpFormatter
)
parser.add_argument('--annotated_sv_file', required=True)
parser.add_argument('--gene_list_file', required=True)
parser.add_argument('-S', '--field_separator', default='tab',
                    help='Field separator: "comma", "tab", or a single character like ";" or "|"')
parser.add_argument('--spark_executor_mem', type=int, default=4, help='Spark executor memory in GB')
parser.add_argument('--spark_executor_instance', type=int, default=1, help='Number of executors')
parser.add_argument('--spark_executor_core', type=int, default=1, help='Number of executor cores')
parser.add_argument('--spark_driver_maxResultSize', type=int, default=1,
                    help='Driver max result size in GB')
parser.add_argument('--sql_broadcastTimeout', type=int, default=300,
                    help='SQL broadcast timeout in seconds')
parser.add_argument('--spark_driver_core', type=int, default=1, help='Driver cores')
parser.add_argument('--spark_driver_mem', type=int, default=4, help='Driver memory in GB')
parser.add_argument('--filter_gene_name', action='store_true', help='Enable filtering by Gene_name')
parser.add_argument('--filter_re_gene', action='store_true', help='Enable filtering by RE_gene')
args = parser.parse_args()

if not (args.filter_gene_name or args.filter_re_gene):
    parser.error("At least one of --filter_gene_name or --filter_re_gene must be enabled")

# 2. Create Spark session with Glow enabled
spark = SparkSession.builder.appName('glow_pyspark') \
    .config('spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.1.0,io.projectglow:glow-spark3_2.12:2.0.0') \
    .config('spark.jars.excludes',
            'org.apache.hadoop:hadoop-client,io.netty:netty-all,io.netty:netty-handler,io.netty:netty-transport-native-epoll') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.debug.maxToStringFields', '0') \
    .config('spark.hadoop.io.compression.codecs', 'io.projectglow.sql.util.BGZFCodec') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider',
            'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider') \
    .config('spark.kryoserializer.buffer.max', '512m') \
    .config('spark.executor.memory', f'{args.spark_executor_mem}G') \
    .config('spark.executor.instances', args.spark_executor_instance) \
    .config('spark.executor.cores', args.spark_executor_core) \
    .config('spark.driver.maxResultSize', f'{args.spark_driver_maxResultSize}G') \
    .config('spark.sql.broadcastTimeout', args.sql_broadcastTimeout) \
    .config('spark.driver.cores', args.spark_driver_core) \
    .config('spark.driver.memory', f'{args.spark_driver_mem}G') \
    .getOrCreate()

spark = glow.register(spark)

# 3. File paths and separator
gene_text_path = args.gene_list_file
annotated_sv_file = args.annotated_sv_file
separator_map = {'comma': ',', 'tab': '\t', 'semicolon': ';', 'pipe': '|'}
sep = separator_map.get(args.field_separator.lower(), args.field_separator)

# 4. Load gene list into Python list
gene_symbols_trunc = spark.read.option("header", False).text(gene_text_path)
gene_symbols_trunc = list(gene_symbols_trunc.toPandas()['value'])
gene_symbols_trunc = [g.replace('\xa0', '').replace('\n', '') for g in gene_symbols_trunc]

# 5. Load structural variant table
sv_df = spark.read.options(inferSchema=True, sep=sep, header=True, nullValue="").csv(annotated_sv_file)

sv_df_genes = sv_df.withColumn("Gene_name_split_col",
                               explode(split(coalesce(col("Gene_name"), lit("")), ";"))) \
    .withColumn("RE_gene_split_col",
                explode(split(coalesce(col("RE_gene"), lit("")), ";"))) \
    .withColumn("RE_gene_symbol",
                trim(regexp_extract(col("RE_gene_split_col"), r'^([^\s(]+)', 1)))

# 6. Build filter conditions based on flags
conditions = []
if args.filter_gene_name:
    conditions.append(col("Gene_name_split_col").isin(gene_symbols_trunc))
if args.filter_re_gene:
    conditions.append(col("RE_gene_symbol").isin(gene_symbols_trunc))

filtered = sv_df_genes
if conditions:
    combined = reduce(lambda a, b: a | b, conditions)
    filtered = filtered.filter(combined)

# 7. Output path
base = os.path.basename(annotated_sv_file)
name, ext = os.path.splitext(base)
output_file = f"{name}.filtered{ext}"

# 8. Handle empty result
if filtered.isEmpty(): 
    print(f"No matching rows found — wrote empty file: {output_file}")
    sys.exit(0)
else:
    filtered.drop('Gene_name_split_col', 'RE_gene_split_col', 'RE_gene_symbol') \
        .distinct() \
        .toPandas() \
        .to_csv(output_file, sep="\t", index=False, na_rep='-')
    print(f"Filtered results written to {output_file}")
