#!/bin/bash

# Linda de Vries
# assignment 3
# shell: sbatch assignment3.sh

#BATCH --mail-user=ldevries@bioinf.nl
#BATCH --mail-type=END
#SBATCH --job-name=assignment3_BDC_Linda
#SBATCH --output=res.txt
#SBATCH --account=ldevries
#SBATCH --partition=assemblix
#SBATCH --nodes=1

source /commons/conda/conda_load.sh

cores=16

### NOTE: het ging crashem na een paar uur. dus vandaar met data wat wel lukte.
#export index=/students/2021-2022/Thema11/ldevries/data/homo_sapiens.fa
#export data=/students/2021-2022/Thema11/ldevries/data/lupus.fa

for n in $(seq 1 $cores);
do
/usr/bin/time -o timings.txt --append -f "${n}\t%e" minimap2 /data/dataprocessing/MinIONData/all_bacteria.fna /data/dataprocessing/MinIONData/all.fq -t "${n}" -a > /dev/null
done