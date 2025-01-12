tmux new-session -d -s "vmstorage1" ./bin/vmstorage -storageDataPath=./ -retentionPeriod=100d
tmux new-session -d -s "vmsketch1" ./bin/vmsketch 
tmux new-session -d -s "vmselect1" ./bin/vmselect -storageNode=127.0.0.1:8400 -sketchNode=127.0.0.1:8500  
tmux new-session -d -s "vminsert1" ./bin/vminsert -storageNode=127.0.0.1:8400 -sketchNode=127.0.0.1:8500
