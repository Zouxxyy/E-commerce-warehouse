#!/bin/bash

# 获取参数个数，如果没有参数，则直接退出
pcount=$#
if ((pcount == 0)); then
  echo no args
  exit
fi

# 获取文件名称
p1=$1
fname=$(basename $p1)
echo fname=$fname

# 获取上级目录到绝对路径
pdir=$(
  cd -P $(dirname $p1)
  pwd
)
echo pdir=$pdir

# 获取当前用户名称
user=$(whoami)

array=([0]="dell-r720" [1]="dell-r730-4" [2]="dell-r730-5")

# 循环
for i in dell-r720 dell-r730-4 dell-r730-5; do
  echo --------- $i ----------
  rsync -rvl $pdir/$fname $user@$i:$pdir
done
