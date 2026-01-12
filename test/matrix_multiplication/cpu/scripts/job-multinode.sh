#!/bin/bash

#SBATCH -J matmul
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --time=01:00:00
#SBATCH -o logs/log-%N.%J.out

# the module section; load the necessary modules here:
module purge
module load oneapi/2023.0.0
module load impi/oneapi-2023.0.0

# define the directory in which the executable resides:
export EXE_DIR=/wrk/rvc1

# clean work dir
srun --ntasks-per-node=1 rm -rf $EXE_DIR/*

# copy files
#sbcast ./prog $EXE_DIR/matmul
cp ./prog $EXE_DIR/matmul
srun --ntasks-per-node=1 ls -al $EXE_DIR

# export I_MPI_PMI_LIBRARY=/usr/lib64/libpmix.so

# program
MATRIX_SIZE=10000
TILE_SIZE=1024
POOL_SIZE=2000
mpirun -n $SLURM_NNODES --map-by node \
	$EXE_DIR/matmul \
	-M $MATRIX_SIZE -N $MATRIX_SIZE -K $MATRIX_SIZE \
	-tileSize $TILE_SIZE \
	-poolSize $POOL_SIZE \
	-threads $SLURM_CPUS_PER_TASK
