`vmsketch` performs the following tasks:

- Accepts inserts from `vminsert` nodes and stores them to local sketch instances in memory.

- Performs select requests from `vmselect` nodes.

Please note, vmsketch service is listening on :8500 for vminsert connections (see -vminsertAddr flag), on :8501 for vmselect connections (see --vmselectAddr flag) and on :8582 for HTTP connections (see -httpListenAddr flag).