# DeleteKeysFromList
Utility to read keys from a file and attempt to delete them from a given bucket

Sample Output

    I see 3 arguments
    I will read from the file:  /tmp/keylist/keylist1
    I will connect to the host: 172.23.99.170
    I will access the bucket:   BUCKETNAME
    Not using a bucket password
    Cluster and bucket connections established in 896 ms.
    I have read 3 keys into memory in 0 ms and will start processing them now.
    Working on key #0 : key1
    Working on key #1 : key2
    Working on key #2 : key3
    Done processing key list.
    Total number of keys:                  3
    Number that matched deletion criteria: 1
    Total number successfully deleted:     1
    Closing Cluster and bucket connections.

