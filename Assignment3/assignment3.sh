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

### Test data
#index=/students/2021-2022/Thema11/ldevries/data/homo_sapiens.fa
#data=/students/2021-2022/Thema11/ldevries/data/lupus.fa

index=/data/dataprocessing/MinIONData/all_bacteria.fna
data=/data/dataprocessing/MinIONData/all.fq

for ((n = 1; n <= 16; n++)); do
    /usr/bin/time -o timings.txt --append -f "${n}\t%e" minimap2 -t $n+1 -a $index $data > "/dev/null" 2> log.txt
done
