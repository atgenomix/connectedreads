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
# @file    fq_upload.py
#
# @brief   Chunk pair-end FASTQ files into small blocks and upload them to HDFS
#
# @author  Chung-Tsai Su(chungtsai.su@atgenomix.com)
#
# @date    2019/09/18
#
# @version 1.0
#
# @remark
#
import sys
import getopt
import os.path
import yaml
from django.conf import settings
sys.path.append('/usr/local/seqslab/artemis')
settings.configure()
from core.JobController.config_constant import *
from core.WorkflowController.Annotation.AnnotationPreprocessor import *
from core.DatasetsProcessor.FastqChunkwiseChopNUploader import *


def usage():
    print(
        "fq_upload.py -1 <Read 1 FASTQ file> -2 <Read 2 FASTQ file> -o <Output HDFS Folder>")
    print("Argument:")
    print("\t-h: Usage")
    print("\t-1: Input Read 1 FASTQ file")
    print("\t-2: Input Read 2 FASTQ file")
    print("\t-o: Output tsv list")
    print("Usage:")
    print("\t./fq_upload.py -1 ~/NA19240/SRR7782669_same_r1.fq.gz -2 ~/NA19240/SRR7782669_same_r2.fq.gz "
          "-o /NA12878/chunk.fq")

    return


def uploader(i1file, i2file, ofolder):
    uploader = FastqChunkwiseChopNUploader([i1file, i2file], ofolder)
    uploader.run()
    return


def main(argv):
    i1file = ""
    i2file = ""
    ofolder = ""

    try:
        opts, args = getopt.getopt(argv, "h1:2:o:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt == "-1":
            i1file = arg
        elif opt == "-2":
            i2file = arg
        elif opt == "-o":
            ofolder = arg

    # error handling for input parameters
    if i1file == "":
        print("Error: '-1' is required")
        usage()
        sys.exit(2)
    elif not os.path.isfile(i1file):
        print("Error: input file(%s) is not existed" % i1file)
        usage()
        sys.exit(3)
    elif i2file == "":
        print("Error: '-2' is required")
        usage()
        sys.exit(4)
    elif not os.path.isfile(i2file):
        print("Error: input file(%s) is not existed" % i2file)
        usage()
        sys.exit(5)
    elif ofile == "":
        print("Error: '-o' is required")
        usage()
        sys.exit(4)

    # Main Function
    uploader(i1file, i2file, ofolder)

    return


if __name__ == '__main__':
    main(sys.argv[1:])
