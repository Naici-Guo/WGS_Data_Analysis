"""
Filter Variants by Variant (Quality Score) Recalibration.
"""
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
import os
import sys
import glob

@transform("cohort.vcf.gz",
           suffix(".vcf.gz"),
           ["cohort.recal",
            "cohort.tranches",
            "cohort.plots.R"])
def variant_recalibrator(input_file, output_file):
    output_recal_file = output_file[0]
    output_tranches_file = output_file[1]
    output_r_file = output_file[2]
    threads = 1
    statement = '''module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                    gatk VariantRecalibrator
                        -R reference/hs38DH.fa
                        -V %(input_file)s
                        --resource:hapmap,known=false,training=true,truth=true,prior=15.0 reference/hapmap_3.3.hg38.sites.vcf.gz
                        --resource:omni,known=false,training=true,truth=false,prior=12.0 reference/1000G_omni2.5.hg38.sites.vcf.gz
                        --resource:1000G,known=false,training=true,truth=false,prior=10.0 reference/1000G_phase1.snps.high_confidence.hg38.vcf.gz
                        --resource:dbsnp,known=true,training=false,truth=false,prior=2.0 reference/Homo_sapiens_assembly38.dbsnp138.vcf.gz
                        -an QD -an MQ -an MQRankSum -an ReadPosRankSum -an FS -an SOR
                        -mode SNP
                        -O %(output_recal_file)s
                        --tranches-file %(output_tranches_file)s
                        --rscript-file %(output_r_file)s'''
    P.run(statement, job_memory="4G", job_threads=threads)

# this step doesn't remove the variants that don't pass the criteriar but just add a label saying not pass (not sure)
@follows(variant_recalibrator)
@transform("cohort.vcf.gz",
           suffix('.vcf.gz'),
           'calibrated.cohort.vcf.gz')
def apply_vqsr(input_file, output_file):
    threads = 1
    statement = ''' module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                    gatk ApplyVQSR
                        -R reference/hs38DH.fa
                        -V %(input_file)s
                        -O %(output_file)s
                        --truth-sensitivity-filter-level 99.9
                        --tranches-file cohort.tranches
                        --recal-file cohort.recal'''
    P.run(statement, job_memory="4G", job_threads=threads)

@follows(apply_vqsr)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
