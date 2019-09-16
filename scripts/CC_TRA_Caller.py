#!/bin/env python
#
# @note Copyright (C) 2019, Atgenomix Incorporated. All Rights Reserved.
#       This program is an unpublished copyrighted work which is proprietary to
#       Atgenomix Incorporated and contains confidential information that is not to
#       be reproduced or disclosed to any other person or entity without prior
#       written consent from Atgenomix, Inc. in each and every instance.
#
# @warning Unauthorized reproduction of this program as well as unauthorized
#          preparation of derivative works based upon the program or distribution of
#          copies by sale, rental, lease or lending are violations of federal copyright
#          laws and state trade secret laws, punishable by civil and criminal penalties.
#
# @file    CC_TRA_Caller.py
#
# @brief   Parsing BAM file to extract large Translocations for Connected-Reads and estimate the amount of them
#
# @author  Chung-Tsai Su(chungtsai.su@atgenomix.com)
#
# @date    2019/06/03
#
# @version 1.0
#
# @remark
#

import sys
import getopt
import os.path
import re
import pysam  # http://pysam.readthedocs.org/en/latest/api.html
from collections import defaultdict

# Default Parameter
MIN_LENGTH = 50
MIN_OFFSET = 100
MIN_MAPQ = 60
MAX_CIGAR_STRING = 15


def usage():
    print(
        "CC_TRA_Caller.py -i <Input BAM> -l <Minimal length> -f <Minimal offset> -q <Minimal MAPQ> -o <Output tsv list>")
    print("Argument:")
    print("\t-h: Usage")
    print("\t-i: Input BAM file")
    print("\t-l: minimal length for soft/hard clipping (default: %d )" % MIN_LENGTH)
    print("\t-f: minimal offset (default: %d )" % MIN_OFFSET)
    print("\t-q: minimal MAPQ (default: %d )" % MIN_MAPQ)
    print("\t-o: Output tsv list")
    print("Usage:")
    print(
        "\ttime python3 ./CC_TRA_Caller.py -i ~/NA12878-novaseq/v1.0.2/result-1.0.2-qual-fix-6.primary.sorted.bam -l %d -f %d -q %d "
        "-o ~/NA12878-novaseq/v1.0.2/SV.tsv > ~/NA12878-novaseq/v1.0.2/SV.log" % (MIN_LENGTH, MIN_OFFSET, MIN_MAPQ))
    print("\ttime python3 ./CC_TRA_Caller.py -i ../data/NA12878/result-1.0.2-qual-fix-6.primary.sorted.bam -l %d "
          "-f %d -q %d -o ../data/NA12878/SV-L%d-F%d-Q%d.tsv > ../data/NA12878/SV-L%d-F%d-Q%d.log" %
          (MIN_LENGTH, MIN_OFFSET, MIN_MAPQ, MIN_LENGTH, MIN_OFFSET, MIN_MAPQ, MIN_LENGTH, MIN_OFFSET, MIN_MAPQ))

    return


#     | POS | SEQ
# ----+-----+-----
#   M |  +  |  +
#   I |     |  +
#   D |  +  |
#   S |     |  +
#   H |     |


def analyzer(ifn, min_length, min_offset, min_mapq, ofn):
    samfile = pysam.AlignmentFile(ifn, "rb")
    total_contigs = 0
    num_tra = 0
    num_distinct_tra = 0

    h_clipping_chr = defaultdict(str)
    h_clipping_pos = defaultdict(str)
    h_clipping_head = defaultdict(bool)
    h_clipping_breakpoint = defaultdict(int)
    h_clipping_cigar = defaultdict(str)
    output_tra = open(ofn + "_tra.q" + str(min_mapq) + ".tsv", "w")
    num_support = 0
    support_chr = ""
    support_pos = 0
    supports = ""

    # mapped reads for Deletion
    for read in samfile.fetch():
        if read.reference_name == 'chrM':
            break

        if any(re.findall(r'H|S', str(read.cigarstring), re.IGNORECASE)) and read.mapping_quality >= min_mapq:
            # print("%s" % read.cigarstring)
            items = re.split("([0-9]+\S)", str(read.cigarstring))
            if len(items) >= MAX_CIGAR_STRING * 2:
                continue

            if h_clipping_chr[read.query_name] == "":
                if ("H" in items[1] or "S" in items[1]) and int(items[1][:-1]) >= min_length:
                    h_clipping_chr[read.query_name] = read.reference_name
                    h_clipping_pos[read.query_name] = int(items[1][:-1])
                    h_clipping_head[read.query_name] = True
                    h_clipping_breakpoint[read.query_name] = read.reference_start + 1
                    h_clipping_cigar[read.query_name] = read.cigarstring
                elif ("H" in items[len(items) - 2] or "S" in items[len(items) - 2]) \
                        and int(items[len(items) - 2][:-1]) >= min_length:
                    h_clipping_chr[read.query_name] = read.reference_name
                    h_clipping_pos[read.query_name] = int(items[len(items) - 2][:-1])
                    h_clipping_head[read.query_name] = False
                    h_clipping_breakpoint[read.query_name] = read.reference_start + read.reference_length
                    h_clipping_cigar[read.query_name] = read.cigarstring
            elif h_clipping_chr[read.query_name] != read.reference_name:
                l = 0
                for i in range(len(items)):
                    if "M" in items[i]:
                        l += int(items[i][:-1])

                if ("H" in items[len(items) - 2] or "S" in items[len(items) - 2]) \
                        and int(items[len(items) - 2][:-1]) >= min_length \
                        and abs(l - h_clipping_pos[read.query_name]) <= min_offset:
                    buf = "%s\t%s:%d\t%s\t%s\t%s:%d\t%s\t%s\n" % (read.query_name,
                                                                  h_clipping_chr[read.query_name],
                                                                  h_clipping_breakpoint[read.query_name],
                                                                  h_clipping_cigar[read.query_name],
                                                                  h_clipping_head[read.query_name],
                                                                  read.reference_name,
                                                                  read.reference_start + read.reference_length,
                                                                  str(read.cigarstring),
                                                                  False)
                    if support_chr != read.reference_name \
                            or abs(support_pos - read.reference_start - read.reference_length) > 10 * min_offset:
                        if num_support > 1:
                            output_tra.write(supports)
                            num_tra += num_support
                            num_distinct_tra += 1
                        num_support = 1
                        support_chr = read.reference_name
                        support_pos = read.reference_start + read.reference_length
                        supports = buf
                    else:
                        num_support += 1
                        supports += buf
                        # print("%s\t%d\t%s\t%d" % (read.query_name, num_support, support_chr, support_pos))

                elif ("H" in items[1] or "S" in items[1]) \
                        and int(items[1][:-1]) >= min_length \
                        and abs(l - h_clipping_pos[read.query_name]) <= min_offset:
                    buf = "%s\t%s:%d\t%s\t%s\t%s:%d\t%s\t%s\n" % (read.query_name,
                                                                  h_clipping_chr[read.query_name],
                                                                  h_clipping_breakpoint[read.query_name],
                                                                  h_clipping_cigar[read.query_name],
                                                                  h_clipping_head[read.query_name],
                                                                  read.reference_name,
                                                                  read.reference_start + 1,
                                                                  str(read.cigarstring),
                                                                  True)
                    if support_chr != read.reference_name \
                            or abs(support_pos - read.reference_start - 1) > 10 * min_offset:
                        if num_support > 1:
                            output_tra.write(supports)
                            num_tra += num_support
                            num_distinct_tra += 1
                        num_support = 1
                        support_chr = read.reference_name
                        support_pos = read.reference_start + 1
                        supports = buf
                    else:
                        num_support += 1
                        supports += buf
                        # print("%s\t%d\t%s\t%d" % (read.query_name, num_support, support_chr, support_pos))

        total_contigs += 1
    if num_support > 1:
        output_tra.write(supports)
        num_tra += num_support

    print("There are %d contigs" % total_contigs)
    print("There are %d potential TRA" % num_tra)
    print("There are %d distinct TRA" % num_distinct_tra)
    output_tra.close()
    samfile.close()
    return


def main(argv):
    ifile = ""
    ofile = ""
    min_length = MIN_LENGTH
    min_offset = MIN_OFFSET
    min_mapq = MIN_MAPQ

    try:
        opts, args = getopt.getopt(argv, "hi:l:f:q:o:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt == "-i":
            ifile = arg
        elif opt == "-l":
            min_length = int(arg)
        elif opt == "-f":
            min_offset = int(arg)
        elif opt == "-q":
            min_mapq = int(arg)
        elif opt == "-o":
            ofile = arg

    # error handling for input parameters
    if ifile == "":
        print("Error: '-i' is required")
        usage()
        sys.exit(2)
    elif not os.path.isfile(ifile):
        print("Error: input file(%s) is not existed" % ifile)
        usage()
        sys.exit(3)
    if ofile == "":
        ofile = "%s.tsv" % ifile

    # Main Function
    analyzer(ifile, min_length, min_offset, min_mapq, ofile)

    return


if __name__ == '__main__':
    main(sys.argv[1:])
