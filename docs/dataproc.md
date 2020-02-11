# ConnectedReads Quick Start on Google Cloud

This is an explanation of how to launch DeepVariant-on-Spark in Google Cloud.

## Background

Google Cloud Dataproc (Cloud Dataproc) is a cloud-based managed Spark and Hadoop 
service offered on Google Cloud Platform.

## Preliminaries
To access DataProc, plese install `gsutil` first. You can go to
[Google Cloud](https://cloud.google.com/storage/docs/gsutil_install) 
for installation guide.

For password-less deployment, your SSH key is required. Please refer to
[this link](https://cloud.google.com/compute/docs/instances/adding-removing-ssh-keys)
for acquiring your SSH Key.

## Launch Cluster

```
gcloud beta dataproc clusters create my-connectedreads \
 --subnet default --zone us-west1-b \
 --num-workers 2 --worker-machine-type n1-highmem-16 \
 --image-version 1.2.59-deb9 \
 --initialization-actions gs://seqslab-deepvariant/scripts/initialization-on-dataproc.sh \
 --initialization-action-timeout 20m
```

## Delete Cluster

```
gcloud beta dataproc clusters delete my-connectedreads
```

## ConnectedReads Installation

### Login to the Spark master

For password-less deployment, your SSH key
(i.e. ~/.ssh/google_compute_engine) should be added by using `ssh-add`
first. When the cluster has been launched completely, you can login the
terminal of the master via Google Cloud Platform or the following
command:

```
ssh-add -K ~/.ssh/google_compute_engine
gcloud compute ssh --ssh-flag="-A" my-connectedreads-m --zone="us-west1-b"
```

*Note*: if `ssh-add` is failed and the error message is like "Error
connecting to agent: No such file or directory", please use the
following command first.

```
ssh-agent bash
```

### Download and Build ConnectedReads

ConnectedReads leverages mvn to build its package. Please clone ConnectedReads github
repo. and use mvn to build all of modules followed by the following commands:

```
git clone https://github.com/atgenomix/connectedreads.git
cd connectedreads
mvn package
```

### Run ConnectedReads

Please upload your FASTQ files into the Spark master and edit [scripts/run.sh](scripts/run.sh). 
Please refer to  [ConnectedReads WGS case study](docs/wgs-case-study.md) for more details. 

### Status Monitor 

Please refer to [Cluster Operation Portal](trobuleshooting.md#how-to-monitor-the-progress-of-the-pipeline-)
to monitor the healthy status of YARN and HDFS.