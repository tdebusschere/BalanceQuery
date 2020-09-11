set arg1=%1
shift

call C:\Users\DS.Tom\Anaconda3\condabin\conda.bat activate
python C:\Users\DS.Tom\Desktop\Project\BalanceCenterHourlyQuery\DS_SQLGetDF_fail_schedule_v6.py --server=%arg1%
exit
