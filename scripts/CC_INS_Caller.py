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
# @file    CC_INS_Caller.py
#
# @brief   Parsing BAM file to extract large Insertions for Connected-Reads and estmiate the amount of them
#
# @author  Chung-Tsai Su(chungtsai.su@atgenomix.com)
#
# @date    2019/07/17
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

# CONST Parameter
DIVISOR = 100
# MAX_CIGAR_STRING = 15

# Default Parameter
MIN_LENGTH = 50
MIN_OFFSET = 100
MIN_MAPQ = 60


def usage():
    print(
        "CC_INS_Caller.py -i <Input BAM> -l <Minimal length> -f <Minimal offset> -q <Minimal MAPQ> -o <Output VCF File>")
    print("Argument:")
    print("\t-h: Usage")
    print("\t-i: Input BAM file")
    print("\t-l: minimal length for soft/hard clipping (default: %d )" % MIN_LENGTH)
    print("\t-f: minimal offset (default: %d )" % MIN_OFFSET)
    print("\t-q: minimal MAPQ (default: %d )" % MIN_MAPQ)
    print("\t-o: Output VCF file")
    print("Usage:")
    print(
        "\tpython3 ./CC_INS_Caller.py -i ~/NA12878-novaseq/v1.0.2/result-1.0.2-qual-fix-6.primary_alt.sorted.bam "
        "-l %d -f %d -q %d -o ~/NA12878-novaseq/v1.0.2/result-1.0.2-qual-fix-6.primary_alt.sorted.vcf" %
        (MIN_LENGTH, MIN_OFFSET, MIN_MAPQ))

    return


def ins_caller(ifn, min_length, min_offset, min_mapq, ofn):
    samfile = pysam.AlignmentFile(ifn, "rb")

    num_insertions = 0

    num_cc_ins = 0
    num_pc_ins = 0
    num_pd_ins = 0

    h_hits = defaultdict(list)
    h_candidates = defaultdict(int)
    h_lens = defaultdict(int)
    h_clipping = defaultdict(int)
    h_orientation = defaultdict(int)
    output = open(ofn, "w")

    # mapped reads for Insertion
    for read in samfile.fetch():
        if read.reference_name == 'chrM':
            break

        if any(re.findall(r'I', str(read.cigarstring), re.IGNORECASE)) and read.mapping_quality >= min_mapq:
            items = re.split("([0-9]+\S)", str(read.cigarstring))
            # if len(items) >= MAX_CIGAR_STRING * 2:
            #     continue

            idx = 0
            is_matched = 0

            for i in range(len(items)):
                if items[i] == "":
                    continue

                l = int(items[i][:-1])
                if "I" in items[i] and l >= min_length:
                    is_matched += 1
                    key = "%s:%d" % (read.reference_name, int((read.reference_start+idx) / DIVISOR))
                    pos = "%s:%d" % (read.reference_name, read.reference_start+idx)
                    if pos not in h_hits[key]:
                        h_hits[key].append(pos)
                    h_candidates[pos] += 1
                    h_lens[pos] = l

                if "M" in items[i] or "D" in items[i]:
                    idx += l

            if is_matched > 0:
                num_insertions += 1

    print("There are %d completely connected insertions" % len(h_candidates.keys()))

    # mapped reads for Hard/Soft-Clipping
    for read in samfile.fetch():
        if read.reference_name == 'chrM':
            break

        if any(re.findall(r'H|S', str(read.cigarstring), re.IGNORECASE)) and read.mapping_quality >= min_mapq:
            items = re.split("([0-9]+\S)", str(read.cigarstring))
            # if len(items) >= MAX_CIGAR_STRING * 2:
            #     continue

            l = 0
            idx = 0
            key = ""
            left_key = ""
            right_key = ""
            pos = ""
            if ("H" in items[1] or "S" in items[1]) and int(items[1][:-1]) >= min_length / 2 \
                    and ("H" in items[len(items) - 2] or "S" in items[len(items) - 2]) \
                    and int(items[len(items) - 2][:-1]) >= min_length / 2:
                key = "%s:%09d" % (read.reference_name, int(read.reference_start / DIVISOR))
                pos = "%s:%d" % (read.reference_name, read.reference_start)
                idx = read.reference_start
                l = int(items[1][:-1])
                h_orientation[pos] |= 4     # both-side clipping
                left_key = "%s:%09d" % (read.reference_name, int(read.reference_start / DIVISOR) - 1)
                right_key = "%s:%09d" % (read.reference_name, int(read.reference_start / DIVISOR) + 1)

            elif ("H" in items[1] or "S" in items[1]) and int(items[1][:-1]) >= min_length / 2:
                key = "%s:%09d" % (read.reference_name, int(read.reference_start / DIVISOR))
                pos = "%s:%d" % (read.reference_name, read.reference_start)
                idx = read.reference_start
                l = int(items[1][:-1])
                h_orientation[pos] |= 1     # left-side clipping
                left_key = "%s:%09d" % (read.reference_name, int(read.reference_start / DIVISOR) - 1)
                right_key = "%s:%09d" % (read.reference_name, int(read.reference_start / DIVISOR) + 1)
            elif ("H" in items[len(items) - 2] or "S" in items[len(items) - 2]) \
                    and int(items[len(items) - 2][:-1]) >= min_length / 2:
                key = "%s:%09d" % (read.reference_name, int((read.reference_start + read.reference_length - 1) / DIVISOR))
                pos = "%s:%d" % (read.reference_name, read.reference_start + read.reference_length - 1)
                idx = read.reference_start + read.reference_length - 1
                l = int(items[len(items) - 2][:-1])
                h_orientation[pos] |= 2     # right-side clipping
                left_key = "%s:%09d" % (read.reference_name, int((read.reference_start + read.reference_length - 1) / DIVISOR) - 1)
                right_key = "%s:%09d" % (read.reference_name, int((read.reference_start + read.reference_length - 1) / DIVISOR) + 1)

            if l > 0:
                found = 0
                if pos in h_candidates:
                    h_candidates[pos] += 1
                    found = 1
                elif key in h_hits or left_key in h_hits or right_key in h_hits:
                    for k in h_hits[key]:
                        if k in h_candidates and abs(int(k.split(":")[1]) - idx) <= min_offset:
                            h_candidates[k] += 1
                            found = 1
                            print("Found1\t%d\t%s\t%s" % (idx, k, h_hits[key]))
                    for k in h_hits[left_key]:
                        if k in h_candidates and abs(int(k.split(":")[1]) - idx) <= min_offset:
                            h_candidates[k] += 1
                            found = 1
                            print("Found2\t%d\t%s\t%s" % (idx, k, h_hits[left_key]))
                    for k in h_hits[right_key]:
                        if k in h_candidates and abs(int(k.split(":")[1]) - idx) <= min_offset:
                            h_candidates[k] += 1
                            found = 1
                            print("Found3\t%d\t%s\t%s" % (idx, k, h_hits[right_key]))
                if found == 0:
                    h_clipping[pos] += 1
                    if pos not in h_hits[key]:
                        h_hits[key].append(pos)
                    if h_lens[pos] < l:
                        h_lens[pos] = l

    for k in h_candidates:
        output.write("%s\t%d\t%d\tCompletely Connected\n" % (k, h_lens[k], h_candidates[k]))

    pre_chr = ""
    pre_pos = 0
    pre_orientation = 0
    pre_l = 0
    cur_chr = ""
    cur_pos = ""
    cur_orientation = 0
    cur_l = 0
    h_potential = defaultdict(int)

    for k in sorted(h_hits.keys()):
        orientation = 0
        max_l = 0
        for pos in h_hits[k]:
            if pos not in h_candidates:
                if max_l < h_lens[pos]:
                    max_l = h_lens[pos]
                if h_orientation[pos] >= 3 and max_l >= min_length / 2:
                    output.write("%s\t%d\t%d\tPartially Connected\n" % (pos, max_l, h_candidates[pos]))
                    h_potential[pos] += 1
                    orientation = 0
                    break
                else:
                    orientation |= h_orientation[pos]
                    items = pos.split(":")
                    cur_chr = items[0]
                    cur_pos = int(items[1])
                    cur_orientation = h_orientation[pos]
                    cur_l = h_lens[pos]
                    # output.write("%s\t%d\t%d\n" % (pos, h_candidates[pos], h_orientation[pos]))

        if max_l >= min_length / 2 and orientation == 3:
            pos = "%s:%d" % (cur_chr, cur_pos)
            output.write("%s:%d\t%d\t2\tPartially Connected\n" % (cur_chr, cur_pos, max_l))
            h_potential[pos] += 1
            cur_chr = ""
            cur_pos = 0
            cur_orientation = 0
            cur_l = 0
        elif cur_chr == pre_chr and cur_pos - pre_pos <= min_offset and cur_orientation + pre_orientation == 3:
            if cur_l > pre_l:
                max_l = cur_l
            else:
                max_l = pre_l
            pos1 = "%s:%d" % (cur_chr, pre_pos)
            pos2 = "%s:%d" % (cur_chr, cur_pos)
            output.write("%s:%d-%d\t%d\tPartially Connected\n" % (cur_chr, pre_pos, cur_pos, max_l))
            h_potential[pos1] += 1
            h_potential[pos2] += 1
            cur_chr = ""
            cur_pos = 0
            cur_orientation = 0
            cur_l = 0
        else:
            pos = "%s:%d" % (cur_chr, cur_pos)
            if pre_pos != 0 and pre_l >= min_length / 2 and pos not in h_potential:
                if pre_orientation == 1:
                    output.write("%s:%d\t%d\tLeft-Clipping\tPotential Detectable\n" % (pre_chr, pre_pos, pre_l))
                elif pre_orientation == 2:
                    output.write("%s:%d\t%d\tRight-Clipping\tPotential Detectable\n" % (pre_chr, pre_pos, pre_l))
                h_potential[pos] += 1

            pre_chr = cur_chr
            pre_pos = cur_pos
            pre_orientation = cur_orientation
            pre_l = cur_l

    output.close()
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
    ins_caller(ifile, min_length, min_offset, min_mapq, ofile)

    return


if __name__ == '__main__':
    main(sys.argv[1:])
