# ConnectedReads Quick Start on Microsoft Azure

This is an explanation of how to launch ConnectedReads in Microsoft Azure.

## Background

Step 0. create Azure Batch account & Storage account
Step 1. install aztk: `pip install aztk`
Step 2. init: `aztk spark init`
Step 3. modify .aztk/secrets.yaml, .aztk/core-site.xml, .aztk/spark-defaults.conf
Step 4. edit job.yaml
Step 5. submit job by aztk: `aztk spark job submit --id {{ job-id }} --configuration {{ job-yaml-path }}`

