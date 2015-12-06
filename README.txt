Setting up ZooKeeper
----------------------
Following are the highlevel steps needed to setup ZooKeeper.

* Download ZooKeeper: http://apache.arvixe.com/zookeeper/stable/zookeeper-3.4.7.tar.gz
* Extract
* Starting ZooKeeper: cd <ZooKeepr Dir>; ./bin/zkServer.sh start conf/zoo.cfg

Running Tests
----------------------
After ZooKeeper is set up, do the following to run my tests.

* Download my source and extract, or get it from github -> git clone https://github.com/prayagchandran/ZooKeeper-Performance-Tests.git
* Go to the source directory
* Run the Testing class:
        java ­cp .:<path to zookeeper*.jar>:<path to zookeeper lib> Testing ZooKeeper1:Port1,ZooKeeper2:Port2 numThreads
        The exact command in my system to run all tests with 3 ZooKeeper instances and 2 threads is:
                java -cp .:/home/prayag/Downloads/zookeeper-3.4.6/zookeeper-3.4.6.jar:/home/prayag/Downloads/zookeeper-3.4.6/lib/* Testing 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183 2
