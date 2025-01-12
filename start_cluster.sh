./bin/vmstorage -storageDataPath=./ -retentionPeriod=100d
./bin/vmsketch 
./bin/vmselect -storageNode=127.0.0.1:8401 -sketchNode=127.0.0.1:8410
./bin/vminsert -storageNode=127.0.0.1:8401 -sketchNode=127.0.0.1:8410
