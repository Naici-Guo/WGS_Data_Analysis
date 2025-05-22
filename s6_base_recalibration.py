"""
Pre-processing before variant calling - base quality recalibration.
"""

# modules
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
import os
import sys
import glob


# base recalibration
@transform("*.rg.md.bam",
           suffix(".rg.md.bam"),
           ".recal_data.table")
def base_recalibration(input_file, output_file):
    threads = 4
    statement = ''' module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                    gatk BaseRecalibrator
                        -I %(input_file)s
                        -R reference/hs38DH.fa
                        --known-sites reference/Homo_sapiens_assembly38.dbsnp138.vcf
                        --known-sites reference/Homo_sapiens_assembly38.known_indels.vcf.gz
                        -O %(output_file)s'''
    P.run(statement, job_memory="4G", job_threads=threads)

# apply bqsr
@transform(base_recalibration,
           suffix(".recal_data.table"),
           add_inputs(".rg.md.bam"),
           ".bqsr.bam")
def apply_bqsr(input_file, output_file):
    threads = 4
    recal_table = input_file[0]
    bam_file = input_file[1]
    statement = '''module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                   gatk ApplyBQSR
                   -R reference/hs38DH.fa
                   -I %(bam_file)s
                   --bqsr-recal-file %(recal_table)s
                   -O %(output_file)s'''
    P.run(statement, job_memory="4G", job_threads=threads)

@follows(apply_bqsr)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
