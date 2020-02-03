## A WGS case study - NA19240

In our publication, we use 3 publicly available WGS samples to demonstrate the performance of 
ConnectedReads. Here, we would like to show you the whole pipeline of ConnectedReads by using 
NA19240 as a case study.

## Data Preparation

NA19240 is available on EMBL-EBI (https://www.ebi.ac.uk/ena/data/view/ERR2438055). You can download its 
 FASTQ files by using the following commands.
 
```
mkdir NA19240
cd NA19240
wget ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR243/005/ERR2438055/ERR2438055_1.fastq.gz
wget ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR243/005/ERR2438055/ERR2438055_2.fastq.gz
```

Please check their size in case network glitch. 

```
user@server# ls -al 
-rw-r--r-- 1 user user 11775908406 Aug  2 11:08 SRR7782669_pass_1.fastq.gz
-rw-r--r-- 1 user user 14621754685 Aug  2 11:28 SRR7782669_pass_2.fastq.gz
```

## Preliminary

Please modify the Spark folder and Spark Master based on your Spark Cluster. 

```
SPARK=/usr/local/spark
SPARK_MASTER=server
```

To simply the interface of the command line, please set the following environment variables.

```
JAR_FOLDER=/seqslab
ADAM_JAR=${JAR_FOLDER}/adam-assembly-1.0.0-qual.jar
CONNECTEDREADS_JAR=${JAR_FOLDER}/connectedreads-1.0.0.jar
CONNECTEDREADS_FOLDER=/usr/local/connectedreads
READ1=./NA19240/SRR7782669_pass_1.fastq.gz
READ2=./NA19240/SRR7782669_pass_2.fastq.gz
HDFS_OUTPUT=./NA19240
CHUNK_FOLDER=${HDFS_OUTPUT}/chunk.fq
ADAM_FOLDER=${HDFS_OUTPUT}/adam
EC_FOLDER=${HDFS_OUTPUT}/ec
SG_FOLDER=${HDFS_OUTPUT}/sg
SG_CHECKPOINT_FOLDER=${HDFS_OUTPUT}/sg-checkpoint
HSA_FOLDER=${HDFS_OUTPUT}/hsa
HSA_CHECKPOINT_FOLDER=${HDFS_OUTPUT}/hsa-checkpoint
TMP_FOLDER=${HDFS_OUTPUT}/tmp
OUTPUT_FASTQ=./NA19240/SRR7782669_ConnectedReads.fastq.gz
```

## Pipeline

### Data Uploader

```
python3 ${CONNECTEDREADS_FOLDER}/script/fq_upload.py -1 ${READ1} -2 ${READ2} -o ${CHUNK_FOLDER}
```

There are 408 SNAPPY files generated from NA19240 and its volume is around 47.8 GB. 

```
user@server# hadoop fs -du -h -s ${CHUNK_FOLDER}
47.8 G  NA19240

user@server# hadoop fs -ls ${CHUNK_FOLDER}
Found 408 items
-rw-r--r--   1 user user  136348659 2019-09-18 07:10 NA19240/part-00000.fastq.snappy
-rw-r--r--   1 user user  129939255 2019-09-18 07:10 NA19240/part-00001.fastq.snappy
-rw-r--r--   1 user user  129645766 2019-09-18 07:10 NA19240/part-00002.fastq.snappy
...
-rw-r--r--   1 user user  214847949 2019-09-18 07:35 NA19240/part-00405.fastq.snappy
-rw-r--r--   1 user user  214440111 2019-09-18 07:35 NA19240/part-00406.fastq.snappy
-rw-r--r--   1 user user   32458990 2019-09-18 07:35 NA19240/part-00407.fastq.snappy
```

### Parallel Data Transformation by Adam

```
${SPARK}/bin/spark-submit \
   --master spark://${SPARK_MASTER}:7077 \
   --class org.bdgenomics.adam.cli.ADAMMain \
   --driver-cores 1 \
   --driver-memory 2g \
   --executor-cores 5 \
   --executor-memory 50g \
   --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
   --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
   --conf "spark.kryo.registrator=org.bdgenomics.adam.serialization.ADAMKryoRegistrator" \
   --conf "spark.hadoop.dfs.replication=1" \
   --conf "spark.eventLog.enabled=true" \
   --conf "spark.dynamicAllocation.enabled=false" \
   ${ADAM_JAR} transformAlignments \
   -force_load_fastq -parquet_compression_codec SNAPPY \
   -tag_reads \
   -trim_both \
   -trim_poly_g \
   -collapse_dup_reads \
   -filter_lc_reads \
   -filter_lq_reads \
   -filter_n \
   -min_quality 20 \
   -max_lq_base 25 \
   -max_N_count 0 \
   ${CHUNK_FOLDER} ${ADAM_FOLDER}
```

Then, the chucked FASTQ files are transformed to ADAM parquet files. 

```
user@server# hadoop fs -du -h ${ADAM_FOLDER}
59.3 G   NA19240/adam/dev-all
123.6 M  NA19240/adam/dev-all_LCfiltered
3.2 G    NA19240/adam/dev-all_LQfiltered
32.6 M   NA19240/adam/dev-all_Nfiltered
```

### ConnectedReads

```
${SPARK}/bin/spark-submit \
   --master spark://${SPARK_MASTER}:7077 \
  --class com.atgenomix.connectedreads.cli.GraphSeqMain \
  --driver-memory 20g \
  --executor-cores 5 \
  --executor-memory 100g\
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
  --conf "spark.dynamicAllocation.enabled=false"  \
  --conf "spark.speculation=false"  \
  --conf "spark.rdd.compress=true"  \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
  --conf spark.kryo.registrator=com.atgenomix.connectedreads.serialization.ADAMKryoRegistrator \
  --conf "spark.hadoop.dfs.replication=1" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.dynamicAllocation.enabled=false" \
  ${CONNECTEDREADS_JAR} correct \
  ${ADAM_FOLDER}/part-r-* \
  ${EC_FOLDER} \
  -keep_err \
  -pl_batch 2 \
  -pl_partition 6 \
  -mlcp 45 \
  -max_read_length 160 \
  -mim_err_depth 40 \
  -min_read_support 3 \
  -max_correction_ratio 0.5 \
  -max_err_read 1 \
  -raw_err_group_len 1 \
  -raw_err_cutoff 1 \
  -seperate_err
  
${SPARK}/bin/spark-submit \
   --master spark://${SPARK_MASTER}:7077 \
    --class com.atgenomix.connectedreads.cli.GraphSeqMain \
    --driver-memory 2g \
    --executor-cores 10 \
    --executor-memory 100g \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
    --conf "spark.hadoop.dfs.replication=1" \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.dynamicAllocation.enabled=false" \
    ${CONNECTEDREADS_JAR} overlap \
    ${EC_FOLDER}/COR.FQ.ADAM/NO.ERR/part-r-* \
    ${SG_FOLDER} \
    -pl_batch 1 \
    -pl_partition 7 \
    -mlcp 85 \
    -rmdup \
    -cache \
    -checkpoint_path ${SG_CHECKPOINT_FOLDER} \
    -max_read_length 151
    
${SPARK}/bin/spark-submit \
  --master spark://${SPARK_MASTER}:7077 \
  --class com.atgenomix.connectedreads.cli.GraphSeqMain  \
  --driver-memory 20g   \
  --executor-cores 10 \
  --executor-memory 100g \
  --conf "spark.driver.maxResultSize=2g" \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --conf "spark.cleaner.periodicGC.interval=15" \
  --conf "spark.cleaner.referenceTracking.cleanCheckpoints=true" \
  --conf "spark.hadoop.dfs.replication=1" \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.dynamicAllocation.enabled=false" \
  ${CONNECTEDREADS_JAR} assemble \
  ${SG_FOLDER}/VT \
  ${SG_FOLDER}/ED/* \
  ${HSA_FOLDER} \
  ${HSA_CHECKPOINT_FOLDER} \
  -input_format PARQUET \
  -mod 3 \
  -small_steps 5 \
  -big_steps 400 \
  -denoise_len 151 \
  -intersection 1 \
  -contig_upper_bound 10 \
  -ploidy 2 \
  -partition 2100 \
  -one_to_one_profiling \
  -ee 40 \
  -fpp 0.001 \
  -degree_profiling
```

Then, You can check the output folder. 

```
root@cgbs:/seqslab/atsai/script# hadoop fs -du -h /NA19240
40.1 G  NA19240/chunk.fq
62.6 G  NA19240/adam
5.5 G   NA19240/ec
41.0 G  NA19240/sg
29.9 G  NA19240/has
```

### Data Downloader

```
${SPARK}/bin/spark-submit \
  --master spark://${SPARK_MASTER}:7077 \
  --class org.bdgenomics.adam.cli.ADAMMain \
  ${ADAM_JAR} transformAlignments \
  -force_load_parquet ${HSA_FOLDER}/result
  ${HSA_FOLDER}/fastq

hadoop fs -text  ${HSA_FOLDER}/fastq/*.snappy | gzip -1 - > ${OUTPUT_FASTQ}

```

Finally, the Haplotype-sensitive contigs of NA19240 is generated. 

```
root@cgbs:/seqslab/atsai/script# ls -al ${OUTPUT_FASTQ}
-rw-r--r-- 1 user        user         8155698350 Aug  3 18:54 SRR7782669_ConnectedReads.fastq.gz
```

**NOTE:** The number of contigs and filesize might have slight differences due to randomly duplication removal. 
