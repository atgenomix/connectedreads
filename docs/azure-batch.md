# ConnectedReads Quick Start on Microsoft Azure

This is an explanation of how to launch ConnectedReads in Microsoft Azure.

## Background

Microsoft Azure is a cloud computing service. Throught AZTK (Azure Distributed Data 
Engineering Toolkit) cloud-based managed Spark and Hadoop service offered on Microsoft 
Azure Platform.

## Preliminaries

*   install ansible: `pip install --user ansible`
*   install aztk: `pip install aztk`

## Launch ConnectedReads via aztk


### Step1. create Azure Batch account & Storage account

### Step 2. initalize aztk
```
aztk spark init
```
Step 3. modify configurations for Spark cluster
```
.aztk/secrets.yaml, .aztk/core-site.xml, .aztk/spark-defaults.conf
```
Step 4. edit zjob.yaml
Step 5. submit job by aztk
```
aztk spark job submit --id {{ job-id }} --configuration {{ job-yaml-path }}
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