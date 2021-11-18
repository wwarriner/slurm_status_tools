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

python -u sstatus.py -c nodes > nodes.csv
python -u sstatus.py -c nodes -s all > node_summary_all.csv
python -u sstatus.py -c nodes -s partitions > node_summary_by_partition.csv
python -u sstatus.py -c load > load.csv
python -u sstatus.py -c partitions > partitions.csv
