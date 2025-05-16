'''
Step2: post-processing after mapping.
Details here: https://github.com/lh3/bwa/blob/master/README-alt.md
'''
from ruffus import *
from cgatcore import pipeline as P
import glob
import os
import sys

# convert *.aln.bam to *.aln.sam
@transform("*.aln.bam",
           suffix(".aln.bam"),
           ".aln.sam")
def convert_bam_to_sam(input_bam, output_sam):
  statement = '''module load SAMtools/1.16.1-GCC-11.3.0 &&
                samtools view -h -o %(output_sam)s %(input_bam)s'''
  P.run(statement,job_memory="16G", job_threads=4)

# run bwa-postalt.js
@transform(convert_bam_to_sam,
           suffix(".aln.sam"),
           ".postalt.bam")
def run_post_alt_js(input_file, output_file):
  statement = '''bwa.kit/k8 bwa.kit/bwa-postalt.js reference/hs38DH.fa.alt %(input_file)s | samtools view -b -o %(output_file)s - '''
  P.run(statement,job_memory="16G", job_threads=4)

@follows(run_post_alt_js)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))