#!/usr/bin/env bash

#####################################################
## NOTE: Please modify these variables before running
SPARK=/usr/local/spark
SPARK_MASTER=server
CONNECTEDREADS_FOLDER=/usr/local/connectedreads
JAR_FOLDER=./your_local_folder
## local disk
ADAM_JAR=${JAR_FOLDER}/adam-assembly-1.0.0-qual.jar
CONNECTEDREADS_JAR=${JAR_FOLDER}/connectedreads-1.0.0.jar
READ1=./NA19240/SRR7782669_pass_1.fastq.gz
READ2=./NA19240/SRR7782669_pass_2.fastq.gz
OUTPUT_FASTQ=./NA19240/SRR7782669_ConnectedReads.fastq.gz
## HDFS
HDFS_OUTPUT=./NA19240
CHUNK_FOLDER=${HDFS_OUTPUT}/chunk.fq
ADAM_FOLDER=${HDFS_OUTPUT}/adam
EC_FOLDER=${HDFS_OUTPUT}/ec
SG_FOLDER=${HDFS_OUTPUT}/sg
SG_CHECKPOINT_FOLDER=${HDFS_OUTPUT}/sg-checkpoint
HSA_FOLDER=${HDFS_OUTPUT}/hsa
HSA_CHECKPOINT_FOLDER=${HDFS_OUTPUT}/hsa-checkpoint
TMP_FOLDER=${HDFS_OUTPUT}/tmp
#####################################################

## Step1: Please upload your FASTQ files on Microsoft Data Lake Storage
#TBD

## Step2: data transformation
aztk spark job submit --id {{ job-id }} --configuration ./conf/data_transofrmation.yaml

## Step3: error correction
aztk spark job submit --id {{ job-id }} --configuration ./conf/error_correction.yaml

## Step4: string graph construction
aztk spark job submit --id {{ job-id }} --configuration ./conf/string_graph.yaml

## Step5: assembly
aztk spark job submit --id {{ job-id }} --configuration ./conf/assembly.yaml

## data export from Parquet files to FASTQ
