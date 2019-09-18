# Usage

## Data Uploader

```
fq_upload.py -1 <Read 1 FASTQ file> -2 <Read 2 FASTQ file> -o <Output HDFS Folder>
Argument:
	-h: Usage
	-1: Input Read 1 FASTQ file
	-2: Input Read 2 FASTQ file
	-o: Output tsv list
Usage:
	./fq_upload.py -1 ~/NA19240/SRR7782669_same_r1.fq.gz -2 ~/NA19240/SRR7782669_same_r2.fq.gz -o /NA19240/chunk.fq
	
```

## Parallel Data Transformation by Adam

```
/usr/local/spark/bin/spark-submit \
  --master spark://server:7077 \
  --class org.bdgenomics.adam.cli.ADAMMain \
  /seqslab/adam-assembly-1.0.0-qual.jar transformAlignments
  
Argument "INPUT" is required
 INPUT                                                           : The ADAM, BAM or SAM file to apply the transforms to
 OUTPUT                                                          : Location to write the transformed data in ADAM/Parquet format
 -add_md_tags VAL                                                : Add MD Tags to reads based on the FASTA (or equivalent) file passed to this option.
 -aligned_read_predicate                                         : Only load aligned reads. Only works for Parquet files. Exclusive of region
                                                                   predicate.
 -allow_one_mismatch_for_each N                                  : trim poly G allow one mismatch for each
 -atgx_transform                                                 : Enable Atgenomix alignment transformation.
 -barcode_len N                                                  : barcode length
 -barcode_whitelist VAL                                          : barcode whitelist
 -bin_quality_scores VAL                                         : Rewrites quality scores of reads into bins from a string of bin descriptions, e.g.
                                                                   0,20,10;20,40,30.
 -cache                                                          : Cache data to avoid recomputing between stages.
 -coalesce N                                                     : Set the number of partitions written to the ADAM output directory
 -collapse_dup_reads                                             : collect reads with same sequences
 -compare_req N                                                  : trim poly G compare req
 -concat VAL                                                     : Concatenate this file with <INPUT> and write the result to <OUTPUT>
 -defer_merging                                                  : Defers merging single file output
 -disable_fast_concat                                            : Disables the parallel file concatenation engine.
 -disable_pg                                                     : Disable writing a new @PG line.
 -disable_sv_dup                                                 : Disable duplication of sv calling reads, soft-clip or discordantly.
 -filter_lc_reads                                                : filter low complexity reads
 -filter_lq_reads                                                : filter out reads containing max_lq_base of base with quality < min_quality
 -filter_n                                                       : filter reads with uncalled base count >= max_N_count, defaulted as 2
 -force_load_bam                                                 : Forces TransformAlignments to load from BAM/SAM.
 -force_load_fastq                                               : Forces TransformAlignments to load from unpaired FASTQ.
 -force_load_ifastq                                              : Forces TransformAlignments to load from interleaved FASTQ.
 -force_load_parquet                                             : Forces TransformAlignments to load from Parquet.
 -force_shuffle_coalesce                                         : Even if the repartitioned RDD has fewer partitions, force a shuffle.
 -h (-help, --help, -?)                                          : Print help
 -known_indels VAL                                               : VCF file including locations of known INDELs. If none is provided, default
                                                                   consensus model will be used.
 -known_snps VAL                                                 : Sites-only VCF giving location of known SNPs
 -lc_kmer N                                                      : Low complexity kmer
 -lc_threshold_len_factor N                                      : Low complexity: threshold = length / lc_threshold_len_factor / lc_kmer
 -limit_projection                                               : Only project necessary fields. Only works for Parquet files.
 -log_odds_threshold N                                           : The log-odds threshold for accepting a realignment. Default value is 5.0.
 -mark_duplicate_reads                                           : Mark duplicate reads
 -max_N_count N                                                  : upper limit for uncalled base count
 -max_consensus_number N                                         : The maximum number of consensus to try realigning a target region to. Default
                                                                   value is 30.
 -max_indel_size N                                               : The maximum length of an INDEL to realign to. Default value is 500.
 -max_lq_base N                                                  : max acceptable low quality base for -filter_lq_reads, defaulted as 10
 -max_mismatch N                                                 : trim poly G max mismatch
 -max_reads_per_target N                                         : The maximum number of reads attached to a target considered for realignment.
                                                                   Default is 20000.
 -max_target_size N                                              : The maximum length of a target region to attempt realigning. Default length is
                                                                   3000.
 -md_tag_fragment_size N                                         : When adding MD tags to reads, load the reference in fragments of this size.
 -md_tag_overwrite                                               : When adding MD tags to reads, overwrite existing incorrect tags.
 -min_acceptable_quality N                                       : Minimum acceptable quality for recalibrating a base in a read. Default is 5.
 -min_length N                                                   : read min length
 -min_quality N                                                  : threshold for low quality base, defaulted as '?', representing Q30 for Illumina
                                                                   1.8+ Phred+33
 -n_mer_len N                                                    : N-mer length
 -paired_fastq VAL                                               : When converting two (paired) FASTQ files to ADAM, pass the path to the second file
                                                                   here.
 -parquet_block_size N                                           : Parquet block size (default = 128mb)
 -parquet_compression_codec [UNCOMPRESSED | SNAPPY | GZIP | LZO] : Parquet compression codec
 -parquet_disable_dictionary                                     : Disable dictionary encoding
 -parquet_logging_level VAL                                      : Parquet logging level (default = severe)
 -parquet_page_size N                                            : Parquet page size (default = 1mb)
 -print_metrics                                                  : Print metrics to the log on completion
 -quality_encode                                                 : encode quality with depth
 -rand_assign_n                                                  : randomly assign N to one of the nucleotides A, C, G, and T
 -realign_indels                                                 : Locally realign indels present in reads.
 -recalibrate_base_qualities                                     : Recalibrate the base quality scores (ILLUMINA only)
 -record_group VAL                                               : Set converted FASTQs' record-group names to this value; if empty-string is passed,
                                                                   use the basename of the input file, minus the extension.
 -reference VAL                                                  : Path to a reference file to use for indel realignment.
 -region_predicate VAL                                           : Only load a specific range of regions. Mutually exclusive with aligned read
                                                                   predicate.
 -repartition N                                                  : Set the number of partitions to map data to
 -single                                                         : Saves OUTPUT as single file
 -sort_fastq_output                                              : Sets whether to sort the FASTQ output, if saving as FASTQ. False by default.
                                                                   Ignored if not saving as FASTQ.
 -sort_lexicographically                                         : Sort the reads lexicographically by contig name, instead of by index.
 -sort_reads                                                     : Sort the reads by referenceId and read position
 -storage_level VAL                                              : Set the storage level to use for caching.
 -stringency VAL                                                 : Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults
                                                                   to LENIENT
 -tag_partition_num N                                            : maximum number of partitions supported by -tag_reads option
 -tag_partition_range N                                          : tag number for a partition
 -tag_reads                                                      : tag read name with serial numbers for coding pair-end information
 -ten_x                                                          : transform 10x format
 -trim_adapter                                                   : trim adapter
 -trim_both                                                      : trim both
 -trim_head                                                      : trim head
 -trim_one                                                       : trim one bp in head and tail
 -trim_poly_g                                                    : trim poly G
 -trim_tail                                                      : trim tail
 -unclip_reads                                                   : If true, unclips reads during realignment.  
```

## ConnectedReads

### Main program
```
/usr/local/spark/bin/spark-submit \
  --master spark://server:7077 \  
  --class com.atgenomix.connectedreads.cli.GraphSeqMain \  
  /seqslab/connectedreads-1.0.0.jar

Usage: connectedreads-submit [<spark-args> --] <annot-args>

Choose one of the following commands:

PREPROCESSING OPERATIONS
             overlap : String Graph Generation
             correct : conduct error correction on input reads

ASSEMBLY OPERATIONS
            assemble : assemble Illumina whole reads via read overlap, read pair, barcode index

```

### Parallel Error Correction
```
/usr/local/spark/bin/spark-submit \  
  --master spark://server:7077 \  
  --class com.atgenomix.connectedreads.cli.GraphSeqMain \  
  /seqslab/connectedreads-1.0.0.jar correct
             
Option "-pl_batch" is required
 INPUT                      : Input path (generated by Adam transform)
 OUTPUT                     : Output path
 -assign_N                  : whether to randeomly replace N base in reads with A/C/G/T base
 -h (-help, --help, -?)     : Print help
 -keep_err                  : whether to keep error report in tsv format
 -max_N_count N             : upper limit for uncalled base count
 -max_correction_ratio N    : maximal error over correction target base ratio for error identification at a certain internal node [default=0.5]
 -max_err_read N            : maximal reads having errors at a certain internal node [default=1]
 -max_read_length N         : Maximal read length [default = 152]
 -mim_err_depth N           : minimal depth of suffix tree where error will be reported [default=40]
 -min_read_support N        : minimal reads support for error identification at a certain internal node [default=3]
 -mlcp N                    : Minimal longest common prefix [default = 45]
 -output_fastq              : whether to dump all reads to fastq file
 -packing_size N            : The number of reads will be packed together [default = 100]
 -pl_batch N                : Prefix length for number of batches [default=2]
 -pl_partition N            : Prefix length for number of partitions [default=6]
 -print_metrics             : Print metrics to the log on completion
 -profiling                 : Enable performance profiling and output to $OUTPUT/STATS
 -raw_err_cutoff N          : minimum threshold of raw error reports
 -raw_err_group_len N       : range where raw error reports should be grouped as a single error event
 -ref_samples_path STRING[] : provide reference sample path with space as deliminator, e.g. path1 path2 ... pathN
 -seperate_err              : separate error [default=true]
 -stats                     : Enable to output statistics of String Graph to $OUTPUT/STATS
 -total_ploidy N            : total ploidy of input fastq file and reference files
```

### Parallel String Graph Construction

```
/usr/local/spark/bin/spark-submit \  
  --master spark://server:7077 \  
  --class com.atgenomix.connectedreads.cli.GraphSeqMain \  
  /seqslab/connectedreads-1.0.0.jar overlap

Option "-pl_batch" is required
 INPUT                  : Input path (generated by Adam transform)
 OUTPUT                 : Output path
 -cache                 : Cache the reads in memory to speedup data processing
 -checkpoint_path VAL   : Checkpoint path
 -h (-help, --help, -?) : Print help
 -max_edges N           : Maximal number of edges per read [default = Integer.MAX_VALUE]
 -max_read_length N     : Maximal read length [default = 152]
 -mlcp N                : Minimal longest common prefix [default = 85]
 -numbering             : Assign an unique number for each read automatically
 -output_fastq          : dump fastq from vertices
 -output_format VAL     : output vertices and edges in format of ASQG | PARQUET
 -packing_size N        : The number of reads will be packed together [default = 100]
 -pl_batch N            : Prefix length for number of batches [default=1]
 -pl_partition N        : Prefix length for number of partitions [default=7]
 -print_metrics         : Print metrics to the log on completion
 -profiling             : Enable performance profiling and output to $OUTPUT/STATS
 -rmdup                 : Remove duplication of reads
 -stats                 : Enable to output statistics of String Graph to $OUTPUT/STATS
```

### Parallel Haplotype-sensitive Assembly (HSA)

```
user@server# /usr/local/spark/bin/spark-submit \  
  --master spark://server:7077 \  
  --class com.atgenomix.connectedreads.cli.GraphSeqMain \  
  /seqslab/connectedreads-1.0.0.jar assemble

Argument "VERTEX_INPUT" is required
 VERTEX_INPUT           : Vertex input path (generated by overlap pipeline)
 EDGE_INPUT             : Edge input path (generated by overlap pipeline)
 OUTPUT                 : Output path
 CHECKPOINT_PATH        : Checkpoint path
 -big_steps N           : Big steps [default=400]
 -contig_upper_bound N  : Contig upper bound
 -degree_profiling      : Degree profiling
 -denoise_len N         : Denoise by contig length
 -ee N                  : Bloom filter expected elements [default=40]
 -fpp N                 : Bloom filter false positive rate [default=0.001]
 -h (-help, --help, -?) : Print help
 -input_format VAL      : input format of vertices/edges [ASQG | PARQUET]
 -intersection N        : Bloom filter intersection
 -mod N                 : Modulo for choosing vertices with label B [default=3]
 -one_to_one_profiling  : One-to-one profiling
 -overlap N             : Overlap length
 -partition N           : Pairing partition [default=2100]
 -ploidy N              : Ploidy of the sample, 2 for human
 -print_metrics         : Print metrics to the log on completion
 -small_steps N         : Small steps [default=5]
```

## Data Downloader
ConnectedReads leverage ADAM to export the haplotype-sensitive contigs to local disk. 

```
/usr/local/spark/bin/spark-submit \
  --master spark://server:7077 \
  --class org.bdgenomics.adam.cli.ADAMMain \
  /seqslab/adam-assembly-1.0.0-qual.jar transformAlignments
  -force_load_parquet ${input assembly parquet folder}
  ${output FASTQ folder}

hadoop fs -text ${output FASTQ folder}/*.snappy | gzip -1 - > ${local FASTQ GZIP file}
```