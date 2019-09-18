#!/usr/bin/env bash

SPARK=/usr/local/spark
SPARK_MASTER=server
CONNECTEDREADS_FOLDER=/usr/local/connectedreads
JAR_FOLDER=/seqslab
ADAM_JAR=${JAR_FOLDER}/adam-assembly-1.0.0-qual.jar
CONNECTEDREADS_JAR=${JAR_FOLDER}/connectedreads-1.0.0.jar
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

hadoop fs -rm -r ${CHUNK_FOLDER}

python3 ${CONNECTEDREADS_FOLDER}/script/fq_upload.py -1 ${READ1} -2 ${READ2} -o ${CHUNK_FOLDER}

hadoop fs -rm -r ${ADAM_FOLDER}

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

hadoop fs -rm -r ${EC_FOLDER}

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


hadoop fs -rm -r ${SG_CHECKPOINT_FOLDER}
hadoop fs -rm -r ${SG_FOLDER}

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

hadoop fs -rm -r ${TMP_FOLDER}
hadoop fs -rm -r ${HSA_CHECKPOINT_FOLDER}
hadoop fs -rm -r ${HSA_FOLDER}

${SPARK}/bin/spark-submit \
  --master spark://${SPARK_MASTER}:7077 \
  --class com.atgenomix.connectedreads.cli.GraphSeqMain  \
  --jars /seqslab/NA12878-novaseq-test/bloom-filter_2.11.jar \
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

${SPARK}/bin/spark-submit \
  --master spark://${SPARK_MASTER}:7077 \
  --class org.bdgenomics.adam.cli.ADAMMain \
  ${ADAM_JAR} transformAlignments \
  -force_load_parquet ${HSA_FOLDER}/result
  ${HSA_FOLDER}/fastq

rm -rf ${OUTPUT_FASTQ}
hadoop fs -text  ${HSA_FOLDER}/fastq/*.snappy | gzip -1 - > ${OUTPUT_FASTQ}
