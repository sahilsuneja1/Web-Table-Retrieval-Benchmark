num_procs=8
max_proc_id=$(($num_procs-1))
#for proc_id in `seq 0 $max_proc_id`;
for proc_id in `seq 3 3`;
do 
	nohup python -u stats.py $num_procs $proc_id >> nohup.out.$proc_id &
done

