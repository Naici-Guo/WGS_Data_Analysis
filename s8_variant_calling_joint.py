"""
Joint variant calling.
"""

# modules
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
import os
import sys
import glob
'''
@follows(single_sample_gvcf_calling)
@collate("gvcf_files/*.g.vcf.gz",
         regex(r"gvcf_files/(.+).g.vcf.gz"),
         "sample_map.txt")
def create_sample_map(input_files, output_file):
    with open(output_file, "w") as outf:
        for gvcf in input_files:
            sample_name = os.path.basename(gvcf).replace(".g.vcf.gz", "")
            outf.write(f"{sample_name}\t{gvcf}\n") 
'''
tmp_dir = os.environ.get("TMPDIR")
# first to use create_sample_map.py to get sample_map.txt
# import single-sample GVCFs into GenomicsDB before joint genotyping
@transform("sample_map.txt",
           suffix(".txt"),
           "cohort_genomicsdb/")
def genomicsdbimport(input_file, output_dir):
    threads = 2
    statement = ''' module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                    gatk --java-options "-Xmx4g -Xms4g" GenomicsDBImport
                         --genomicsdb-workspace-path %(output_dir)s 
                         --sample-name-map %(input_file)s
                         --tmp-dir=%(tmp_dir)s
                         --reader-threads 2'''
    P.run(statement, job_memory="8G", job_threads=threads)

# Perform joint genotyping on all samples pre-called with HaplotypeCaller
@transform(genomicsdbimport,
           formatter(),
           "cohort.vcf.gz")
def genotype_gvcfs(input_dir, output_file):
    threads = 2
    statement = '''module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                   gatk GenotypeGVCFs
                     -R reference/hs38DH.fa
                     -V gendb://%(input_dir)
                     -O %(output_file)s
                     --tmp-dir %(tmp_dir)s'''
    P.run(statement, job_memory="8G", job_threads=threads)
@follows(genotype_gvcfs)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
