#!/bin/bash

#SBATCH -J matmul
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=20
#SBATCH --time=01:00:00
#SBATCH -o logs/log-%N.%J.out

# the module section; load the necessary modules here:
module purge
# module load openmpi/4.1.5/oneapi-2023.0.0-test
# module load lapack/3.11.0/oneapi-2023.0.0
module load impi/oneapi-2025.0.1

# define the directory in which the executable resides:
export EXE_DIR=/wrk/rvc1
cp ./prog $EXE_DIR/matmul
ls -al $EXE_DIR

# change to the directory from which our job will be launched:
echo "submit dir: $SLURM_SUBMIT_DIR"
cd $SLURM_SUBMIT_DIR

# this is the srun line that launches and runs the executable:
srun -n $SLURM_NTASKS $EXE_DIR/matmul  -M 1024 -N 1024 -K 1024 -tileSize 256 -poolSize 1000 -threads 128
