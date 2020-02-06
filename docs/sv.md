# Usage

## Insertion

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

## Translocation

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

