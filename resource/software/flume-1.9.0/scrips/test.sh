#!/bin/bash

# config
bwa_path=/home/lambert/lab/last_year/bwa-paper-test/speed_hust_sentieon/jemalloc/bwamem-lfalive
datas=(ts wes wgs)
export LD_PRELOAD=~/Storage/bin/sentieon-genomics-202010/lib/libjemalloc.so
export MALLOC_CONF=background_thread:true,dirty_decay_ms:40000,metadata_thp:always,lg_tcache_max:16,thp:always

# log
log_name=log/bwa_$(date +%Y_%m_%d_%H:%M:%S).log
touch "${log_name}"
test() {
  /usr/bin/time -v -a -o "${log_name}" "$@"
  sleep 10s
}

# test
for data in "${datas[@]}"; do
  echo ===================="${data}"==================== >>"${log_name}"
  ref2=~/Storage/lambert-ref-index2/hs37d5.fasta
  tumor1=~/Storage/fastq/${data}/merge_data/case/case_1.fastq.gz
  tumor2=~/Storage/fastq/${data}/merge_data/case/case_2.fastq.gz
  normal1=~/Storage/fastq/${data}/merge_data/control/control_1.fastq.gz
  normal2=~/Storage/fastq/${data}/merge_data/control/control_2.fastq.gz
  tumor_output=output/elwg_${data}_tumor.sam
  normal_output=output/elwg_${data}_normal.sam
  test ${bwa_path}/bwa mem -M -t 55 -R "@RG\tID:tumor\tSM:tumor\tLB:tumorLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $tumor1 $tumor2 >$tumor_output
  test ${bwa_path}/bwa mem -M -t 55 -R "@RG\tID:normal\tSM:normal\tLB:normalLib\tPU:runname\tCN:GenePlus\tPL:illumina" $ref2 $normal1 $normal2 >$normal_output
done
