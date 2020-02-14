# ConnectedReads Quick Start on Microsoft Azure

This is an explanation of how to launch ConnectedReads in Microsoft Azure.

## Background

Microsoft Azure is a cloud computing service. Throught AZTK (Azure Distributed Data 
Engineering Toolkit) cloud-based managed Spark and Hadoop service offered on Microsoft 
Azure Platform.

## Preliminaries

*   install ansible: `pip install --user ansible`
*   install aztk: `pip install aztk`
*   build [ConnectedReads package](docs/installation.md)
*   build [Adam package (forked and modified by Atgenomix)](https://github.com/AnomeGAP/adam)

## Launch ConnectedReads via aztk

### Step 1. Create Azure Batch account and Storage account

Please create your Azure Batch account and Storage account first. 

### Step 2. Initalize aztk package
```
aztk spark init
```
### Step 3. Modify configurations for Spark cluster
```
.aztk/secrets.yaml, .aztk/core-site.xml, .aztk/spark-defaults.conf
```
### Step 4. Edit the related configuration file on conf folder

Please specify your JAR files and the FASTQ files you would like to process.

### Step 5. Upload your FASTQ to Microsoft Data Lake Storage

Please upload your data to Microsoft Data Lake Storage and then add the information 
into `scripts/run_on_azure.sh`.

### Step 6. submit job by aztk
```
cd scripts
sh ./run_on_azure.sh
```

## Reference

1. [GitHub](https://github.com/Azure/aztk)
2. [Doc](https://aztk.readthedocs.io/en/latest/index.html)
3. [Batch Pricing](https://docs.microsoft.com/en-us/azure/batch/batch-low-pri-vms)
4. [Run Spark Jobs on Azure Batch](https://medium.com/datamindedbe/run-spark-jobs-on-azure-batch-using-azure-container-registry-and-blob-storage-10a60bd78f90)
5. [Step by Step](https://datainsights.cloud/2018/06/16/how-to-create-a-low-cost-spark-cluster-on-azure/)
6. [FUSE inside Docker](https://stackoverflow.com/questions/48402218/fuse-inside-docker)
7. [Get job API](https://docs.microsoft.com/en-us/rest/api/batchservice/job/get)
8. [Get task API](https://docs.microsoft.com/en-us/rest/api/batchservice/task/get)
9. [Handling preemption](https://docs.microsoft.com/zh-tw/azure/batch/batch-low-pri-vms#handling-preemption)