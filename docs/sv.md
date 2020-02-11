# Structural Variant Pipeline with ConnectedReads

## Motivation

If ConnectedReads generates a mis-assembled contig, then it should be observed via the CIGAR string 
after applying any read mapping tools. For example, two unrelated unitigs are assembled together, the 
contig usually is marked as large soft- or hard-clipping in its CIGAR string. In addition, insertions 
and deletions in the CIGAR string are the candidates should be investigated in the evaluation process.

To evaluate the data correctness of ConnectedReads, the following tools are developed. 

## Preliminaries

Each sample should be process by ConnectedReads and then mapped by minimap2 with default settings. 

## CIGAR-based Insertion Caller for ConnectedReads (CCC-INS)

The tool is adopted and the results on Table 2,3,4 in the manuscript are based on this tool.  

### Usage

```
CC_INS_Caller.py -i <Input BAM> -l <Minimal length> -f <Minimal offset> -q <Minimal MAPQ> -o <Output VCF File>
Argument:
        -h: Usage
        -i: Input BAM file
        -l: minimal length for soft/hard clipping (default: 50 )
        -f: minimal offset (default: 400 )
        -q: minimal MAPQ (default: 60 )
        -o: Output VCF file
Usage:
        python3 ./CC_INS_Caller.py -i ~/NA12878-novaseq/v1.0.2/result-1.0.2-qual-fix-6.primary_alt.sorted.vcf -l 50 -f 400 -q 60 -o ~/NA12878-novaseq/v1.0.2/result-1.0.2-qual-fix-6.primary_alt.sorted.log
        python3 ./CC_INS_Caller.py -i /seqslab/atsai/data/NA12878/result.primary_alt.hg19.sorted.bam -l 50 -f 400 -q 60 -o /seqslab/atsai/data/NA12878/result.primary_alt.hg19.sorted.vcf
```

### Output

There are several examples shown as follows:

```
1       970568  INS     .       .       .       PASS    SVTYPE=INS;SVLEN=25;READS=1;CC=PotentialDetectable(LeftClipping)
1       1004108 INS     .       .       .       PASS    SVTYPE=INS;SVLEN=25;READS=1;CC=PotentialDetectable(LeftClipping)
1       1004213 INS     .       .       .       PASS    SVTYPE=INS;SVLEN=109;READS=2;CC=PartiallyConnected
1       1259552 INS     .       .       .       PASS    SVTYPE=INS;SVLEN=72;READS=2;CC=CompletelyConnected
1       1375825 INS     .       .       .       PASS    SVTYPE=INS;SVLEN=25;READS=1;CC=PotentialDetectable(LeftClipping)
```

## CIGAR-based Translocation-based Insertion Caller for ConnectedReads (CCC-TRA)

The tool is adopted and the results on Table 5 in the manuscript are based on this tool.  

### Usage

```
CC_TRA_Caller.py -i <Input BAM> -l <Minimal length> -f <Minimal offset> -q <Minimal MAPQ> -o <Output tsv list>
Argument:
        -h: Usage
        -i: Input BAM file
        -l: minimal length for soft/hard clipping (default: 50 )
        -f: minimal offset (default: 100 )
        -q: minimal MAPQ (default: 60 )
        -o: Output tsv list
Usage:
        time python3 ./CC_TRA_Caller.py -i ~/NA12878-novaseq/v1.0.2/result-1.0.2-qual-fix-6.primary.sorted.bam -l 50 -f 100 -q 60 -o ~/NA12878-novaseq/v1.0.2/SV.tsv > ~/NA12878-novaseq/v1.0.2/SV.log
        time python3 ./CC_TRA_Caller.py -i ../data/NA12878/result-1.0.2-qual-fix-6.primary.sorted.bam -l 50 -f 100 -q 60 -o ../data/NA12878/SV-L50-F100-Q60.tsv > ../data/NA12878/SV-L50-F100-Q60.log
```

