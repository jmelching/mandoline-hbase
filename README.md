# mandoline-hbase

HBase backend for Mandoline.

This is a work in progress, only some of the tests are currently passing.

# testing

You will need a running HBase database to connect to.  Currently, it only connects to a local
instance.  

Download hbase, extract and start.

 hbase-xxx/bin/start-hbase.sh
 
 lein2 test