#! /bin/bash

#SBATCH --job-name=sstatus

#SBATCH --partition=express
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=3
#SBATCH --mem=4G

#SBATCH --output=%x-%j.log
#SBATCH --error=%x-%j.log

module load Anaconda3
conda activate slurm_load

mkdir -p out
python -u sstatus.py -c nodes | tee out/nodes.csv
python -u sstatus.py -c nodes -s all | tee out/node_summary_all.csv
python -u sstatus.py -c nodes -s partitions | tee out/node_summary_by_partition.csv
python -u sstatus.py -c load | tee out/load.csv
python -u sstatus.py -c load -s partitions | tee out/load_by_partition.csv
python -u sstatus.py -c partitions -f ascii | tee out/partitions.txt
python -u sstatus.py -c partitions -f mediawiki | tee out/partitions.mw
python -u sstatus.py -c partitions -f motd | tee out/partitions.motd
python -u sstatus.py -c partitions -f csv | tee out/partitions.csv
