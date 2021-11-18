# Slurm Status Tools

## Load

Returns neatly formatted tables of data from `scontrol`.

`python -u sstatus.py -c nodes > nodes.csv` - Individual node information.

`python -u sstatus.py -c nodes -s all > node_summary_all.csv` - Node summary across all partitions.

`python -u sstatus.py -c nodes -s partitions > node_summary_by_partition.csv` - Node summary grouped by partition.

`python -u sstatus.py -c load > load.csv` - Percent usage of various resources.

`python -u sstatus.py -c partitions > partitions.csv` - Partition information.
