"""
variant calling per sample.
"""

# modules
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
import os
import sys
import glob

tmp_dir = os.environ.get("TMPDIR")
# single sample variant calling
@follows(mkdir("gvcf_files"))
@transform("*.bqsr.bam",
           suffix('.bqsr.bam'),
           "gvcf_files/\\1.g.vcf.gz")
def single_sample_gvcf_calling(input_file, output_file):
    threads = 2
    job_options = "-t 24:00:00"
    statement = ''' module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                    gatk --java-options "-Xmx4g" HaplotypeCaller
                        -R reference/hs38DH.fa
                        -I %(input_file)s
                        -O %(output_file)s
                        -ERC GVCF'''
    P.run(statement, job_memory="8G", job_threads=threads)

@follows(single_sample_gvcf_calling)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
