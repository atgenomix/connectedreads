# ConnectedReads 

a machine-learning optimized long-range genome analysis workflow for next-generation sequencing

## Background

Current human genome sequencing assays in both clinical and research settings primarily utilize short-read sequencing and apply resequencing pipelines to detect genetic variants. However, structural variant (SV) discovery remains a considerable challenge due to an incomplete reference genome, mapping errors and high sequence divergence. To overcome this challenge, we propose an efficient and effective whole-read assembly workflow with unsupervised graph mining algorithms on an Apache Spark large-scale data processing platform called ConnectedReads. By fully utilizing short-read data information, ConnectedReads is able to generate haplotype-resolved contigs and then streamline downstream pipelines to provide higher-resolution SV discovery than that provided by other methods, especially in N-gap regions. Furthermore, we demonstrate a cost-effective approach by leveraging ConnectedReads to investigate all spectra of genetic changes in population-scale studies.

## Documentation

*   [ConnectedReads release notes](https://github.com/atgenomix/connectedreads/releases)

### Dependence

*   [Apache Hadoop 2.8.x](https://hadoop.apache.org/docs/r2.8.0/)
*   [Apache Spark 2.2.x](https://spark.apache.org/docs/2.2.2/)
*   [Apache Adam v0.23 (forked and modified by Atgenomix)](https://github.com/AnomeGAP/adam)

### Quick start and Case studies

*   [Installation](docs/installation.md)
*   [ConnectedReads WGS case study](docs/wgs-case-study.md)

## Contributing

Interested in contributing? See [CONTRIBUTING](CONTRIBUTING.md).

## License

ConnectedReads is licensed under the terms of the
[Apache 2.0 License](LICENSE).

## Acknowledgements

ConnectedReads happily makes use of many open source packages.
We'd like to specifically call out a few key ones:

*   [Adam -  a genomics analysis platform with specialized file formats
    built using Apache Avro, Apache Spark and
    Parquet.](https://github.com/bigdatagenomics/adam)

We thank all of the developers and contributors to these packages for their
work.


## Disclaimer

*   This is not an official [Atgenomix](https://www.atgenomix.com/) product.
*   To utilize the official product with full experience, please contact Atgenomix (info@atgenomix.com).
