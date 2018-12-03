#!/usr/bin/env bash

export TOP_PID=$$
trap 'exit 2' KILL

RDP_PROJ=$(dirname $(pwd))

RDP_DEPS=$RDP_PROJ/deps

CPU_NUM=`grep processor /proc/cpuinfo | wc -l`                                  
CPU_NUM=`expr $CPU_NUM - 1`
if [ $CPU_NUM -eq 0 ]; then
  CPU_NUM=1
fi

if [ "x$1" == "x" ]
then
echo "USAGE: $0 prepare|3rd_party|comm|syncer_debug|syncer_with_fiu|syncer|syncer_install|topic_translate|fulltopic_check|zk|generate_package"
exit 2
fi

function GetMySQLPkg() {
  mysql_pkg_count=$(ls -l $RDP_PROJ/3rd_party|grep "mysql"|grep -E "tar.gz|tgz"|wc -l)
  if [ 0 == $mysql_pkg_count ]
  then
    echo "no mysql source code package found in dir:$RDP_PROJ/3rd_party" >&2
    kill -s KILL $TOP_PID
  else
    mysql_pkgs=$(ls -l $RDP_PROJ/3rd_party|grep "mysql"|grep -E "tar.gz|tgz"|awk '{print $NF}')
    if [ 1 == $mysql_pkg_count ]
    then
      echo $mysql_pkgs
    else
      mysql_pkgs=`echo $mysql_pkgs|tr -s ' ' '|'`
      read -p "select pkgs[$mysql_pkgs]:"
      echo "input: [$REPLY]" >&2
      if [ ! -f "$RDP_PROJ/3rd_party/$REPLY" ] || [ "x$REPLY" == "x" ]
      then
        echo "input \"$REPLY\" not found in $RDP_PROJ/3rd_party" >&2
        kill -s KILL $TOP_PID
      fi
      echo $REPLY
    fi
  fi
}

function GetMySQLDir() {
  if [[ $1 == *tgz ]]
  then
    echo `basename $1 .tgz`
  else
    echo `basename $1 .tar.gz`
  fi
}

function GetMySQLVersion() {
  if [ ! -f "$RDP_PROJ/3rd_party/$MYSQL_DIR/VERSION" ]
  then
    echo "mysql version file: $RDP_PROJ/3rd_party/$MYSQL_DIR/VERSION not found" >&2
    kill -s KILL $TOP_PID
  fi
  MYSQL_VERSION_MAJOR=`grep MYSQL_VERSION_MAJOR $RDP_PROJ/3rd_party/$MYSQL_DIR/VERSION|awk -F\= '{print $2}'`
  MYSQL_VERSION_MINOR=`grep MYSQL_VERSION_MINOR $RDP_PROJ/3rd_party/$MYSQL_DIR/VERSION|awk -F\= '{print $2}'`
  MYSQL_VERSION_PATCH=`grep MYSQL_VERSION_PATCH $RDP_PROJ/3rd_party/$MYSQL_DIR/VERSION|awk -F\= '{print $2}'`
  echo "${MYSQL_VERSION_MAJOR}_${MYSQL_VERSION_MINOR}_${MYSQL_VERSION_PATCH}"
}

case $1 in
prepare)
source /etc/os-release
case $ID in
debian|ubuntu|devuan)
    apt-get install -y gcc g++ make cmake && \
    apt-get install -y libcurl4-openssl-dev && \
    apt-get install -y libncurses5-dev bison && \
    apt-get install -y python && \
    apt-get install -y libbz2-dev 
    ;;
centos|fedora|rhel)
    yum install -y gcc gcc-c++ make cmake && \
    yum install -y libcurl-devel && \
    yum install -y ncurses-devel bison && \
    yum install -y python
    ;;
esac

;;

3rd_party)
#make & install boost
cd $RDP_PROJ/3rd_party/ && tar -xf boost_1_59_0.tar.gz && cd - && \
cd $RDP_PROJ/3rd_party/boost_1_59_0 && ./bootstrap.sh --prefix=$RDP_DEPS/boost && ./b2 --with-system --with-thread --with-iostreams --with-regex threading=multi link=shared install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install jasson
cd $RDP_PROJ/3rd_party/ && tar -xf jansson-2.10.tar.gz && \
cd $RDP_PROJ/3rd_party/jansson-2.10 && ./configure --prefix=$RDP_DEPS/jansson && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install glog
cd $RDP_PROJ/3rd_party/ && tar -xf glog-0.3.4.tar.gz && \
cd $RDP_PROJ/3rd_party/glog-0.3.4 && ./configure --prefix=$RDP_DEPS/glog && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install librdkafka
cd $RDP_PROJ/3rd_party/ && tar -xf librdkafka-0.9.5-add-thd-name.tar.gz && \
cd $RDP_PROJ/3rd_party/librdkafka-0.9.5-add-thd-name && ./configure --prefix=$RDP_DEPS/librdkafka && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install zookeeper c lib
cd $RDP_PROJ/3rd_party/ && tar -xf zookeeper-3.4.10-add-thd-name-add-log.tar.gz && \
cd $RDP_PROJ/3rd_party/zookeeper-3.4.10-add-thd-name-add-log/src/c && ./configure --prefix=$RDP_DEPS/zklib && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install protobuf
cd $RDP_PROJ/3rd_party/ && tar -xf protobuf-cpp-3.1.0.tar.gz && \
cd $RDP_PROJ/3rd_party/protobuf-3.1.0/ && ./configure --prefix=$RDP_DEPS/protobuf && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install gflags
cd $RDP_PROJ/3rd_party/ && tar -xf gflags-2.2.0.tar.gz && \
cd $RDP_PROJ/3rd_party/gflags-2.2.0/ && cmake . -DINTTYPES_FORMAT=C99 -DCMAKE_INSTALL_PREFIX=$RDP_DEPS/gflags -DBUILD_SHARED_LIBS=ON && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install gtest
cd $RDP_PROJ/3rd_party/ && tar -xf googletest-release-1.8.0.tar.gz && \
cd $RDP_PROJ/3rd_party/googletest-release-1.8.0/ && cmake . -DCMAKE_INSTALL_PREFIX=$RDP_DEPS/gtest && make -j$CPU_NUM && make install && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#make & install lz4
cd $RDP_PROJ/3rd_party/ && tar -zxf lz4-1.8.1.2.tar.gz && \
cd $RDP_PROJ/3rd_party/lz4-1.8.1.2/ && make -j$CPU_NUM -e LDFLAGS=-lrt && make install PREFIX=$RDP_DEPS/lz4 && cd -;
if [ $? -ne 0 ];then
    exit 2
fi

#install intel tbb
cd $RDP_PROJ/3rd_party/;tar -xf tbb2018_20170726oss_lin.tgz;
if [ -d ./tbb ]; then
  rm -r ./tbb
fi
mv -f ./tbb2018_20170726oss ./tbb
if [ -d $RDP_DEPS/tbb ]; then
  rm -r $RDP_DEPS/tbb
fi
mv -f ./tbb $RDP_DEPS/

;;

syncer_debug)

MYSQL_PKG=$(GetMySQLPkg)
MYSQL_DIR=$(GetMySQLDir $MYSQL_PKG)

if [ ! -d $RDP_PROJ/3rd_party/$MYSQL_DIR ]
then
  cd $RDP_PROJ/3rd_party/;tar -xf $MYSQL_PKG; mkdir $RDP_PROJ/3rd_party/$MYSQL_DIR/bld -p;
fi

cd $RDP_PROJ/syncer/client/message && make && make install && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR && rm -r client && ln -s $RDP_PROJ/syncer/client client && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/client && ln -sf $RDP_DEPS deps && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld && cmake .. -DSTAT_TIME=0 -DNO_KAFKA=0 -DCMAKE_BUILD_TYPE=Debug -DRDP_GIT_HASH=`git show | grep "commit" | head -1 | cut -d ' ' -f2` -DENABLE_DOWNLOADS=1 -DWITH_BOOST=$RDP_PROJ/3rd_party/boost_1_59_0/ && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/libmysql && make && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client && make -j$CPU_NUM mysqldump && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client && make -j$CPU_NUM rdp_syncer && cd - && \
cd $RDP_PROJ/tools/save_file_into_mysql/ && make clean && make MYSQL_DIR=$MYSQL_DIR &&  cd -;

;;

syncer)

MYSQL_PKG=$(GetMySQLPkg)
MYSQL_DIR=$(GetMySQLDir $MYSQL_PKG)

if [ ! -d $RDP_PROJ/3rd_party/$MYSQL_DIR ]
then
  cd $RDP_PROJ/3rd_party/;tar -xf $MYSQL_PKG; mkdir $RDP_PROJ/3rd_party/$MYSQL_DIR/bld -p;
fi

cd $RDP_PROJ/syncer/client/message && make && make install && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR && rm -r client && ln -s $RDP_PROJ/syncer/client client && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/client && ln -sf $RDP_DEPS deps && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld && cmake .. -DCMAKE_BUILD_TYPE=Release -DRDP_GIT_HASH=`git show | grep "commit" | head -1 | cut -d ' ' -f2` -DENABLE_DOWNLOADS=1 -DWITH_BOOST=$RDP_PROJ/3rd_party/boost_1_59_0/ && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/libmysql && make && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client && make -j$CPU_NUM mysqldump && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client && make -j$CPU_NUM rdp_syncer && cd - && \
cd $RDP_PROJ/tools/save_file_into_mysql/ && make clean && make MYSQL_DIR=$MYSQL_DIR &&  cd -;

;;

syncer_with_fiu)

MYSQL_PKG=$(GetMySQLPkg)
MYSQL_DIR=$(GetMySQLDir $MYSQL_PKG)

if [ ! -d $RDP_PROJ/3rd_party/$MYSQL_DIR ]
then
cd $RDP_PROJ/3rd_party/;tar -xf $MYSQL_PKG; mkdir $RDP_PROJ/3rd_party/$MYSQL_DIR/bld -p;
fi

cd $RDP_PROJ/syncer/client/message && make && make install && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR && rm -r client && ln -s $RDP_PROJ/syncer/client client && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/client && ln -sf $RDP_DEPS deps && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld && cmake .. -DFIU=1 -DCMAKE_BUILD_TYPE=Debug -DRDP_GIT_HASH=`git show | grep "commit" | head -1 | cut -d ' ' -f2` -DENABLE_DOWNLOADS=1 -DWITH_BOOST=$RDP_PROJ/3rd_party/boost_1_59_0/ && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/libmysql && make && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client && make -j$CPU_NUM mysqldump && cd - && \
cd $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client && make -j$CPU_NUM rdp_syncer && cd - && \
cd $RDP_PROJ/tools/save_file_into_mysql/ && make clean && make MYSQL_DIR=$MYSQL_DIR &&  cd -;

;;

syncer_install)

MYSQL_PKG=$(GetMySQLPkg)
MYSQL_DIR=$(GetMySQLDir $MYSQL_PKG)

if [ ! -d $RDP_PROJ/3rd_party/$MYSQL_DIR ]
then
  echo "$RDP_PROJ/3rd_party/$MYSQL_PKG not compile"
  exit 2
fi

MYSQL_VERSION=$(GetMySQLVersion $MYSQL_DIR)

rm -rf $RDP_PROJ/package/syncer/ && \
mkdir -p $RDP_PROJ/package/syncer/bin/ && \
cp $RDP_PROJ/3rd_party/$MYSQL_DIR/client/*.sh $RDP_PROJ/package/syncer/bin/ && \
cp $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client/rdp_syncer $RDP_PROJ/package/syncer/bin/ && \
cp $RDP_PROJ/3rd_party/$MYSQL_DIR/client/*.cfg.example $RDP_PROJ/package/syncer/ && \
mkdir -p $RDP_PROJ/package/syncer/lib && \
mkdir -p $RDP_PROJ/package/syncer/scripts && \
cp $RDP_PROJ/syncer/client/scripts/* $RDP_PROJ/package/syncer/scripts/ && \
cp $RDP_DEPS/glog/lib/libglog.so.0 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/boost/lib/libboost_system.so.1.59.0 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/boost/lib/libboost_thread.so.1.59.0 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/boost/lib/libboost_regex.so.1.59.0 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/jansson/lib/libjansson.so.4 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/librdkafka/lib/librdkafka++.so.1 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/librdkafka/lib/librdkafka.so.1 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/zklib/lib/libzookeeper_mt.so.2 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/protobuf/lib/libprotobuf.so.11 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/librdp_message/lib/librdp_message.so $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/gflags/lib/libgflags.so.2.2 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/rdp-comm/lib/librdp-comm.so  $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/tbb/lib/intel64/gcc4.7/libtbb.so.2 $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/tbb/lib/intel64/gcc4.7/libtbbmalloc.so.2    $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/tbb/lib/intel64/gcc4.7/libtbb_preview.so.2    $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/libmysql/libmysqlclient.so.* $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_DEPS/lz4/lib/liblz4.so.1    $RDP_PROJ/package/syncer/lib/ && \
cp $RDP_PROJ/tools/user_topic_trans/build/kafka_topic_trans $RDP_PROJ/package/syncer/bin/ && \
cp $RDP_PROJ/tools/fulltopic_check/src/rdp-topic-check $RDP_PROJ/package/syncer/bin/ && \
cp $RDP_PROJ/tools/zk/zk $RDP_PROJ/package/syncer/bin/ && \
cp $RDP_PROJ/tools/save_file_into_mysql/save_file_into_mysql $RDP_PROJ/package/syncer/scripts/ && \
cp $RDP_PROJ/3rd_party/$MYSQL_DIR/bld/client/mysqldump $RDP_PROJ/package/syncer/scripts/ && \
chmod +x $RDP_PROJ/package/syncer/scripts/* && \
chmod +x $RDP_PROJ/package/syncer/bin/*.sh  && \
tar -C $RDP_PROJ/package/syncer  -czvf $RDP_PROJ/package/rdp_mysql_${MYSQL_VERSION}.$(date "+%Y%m%d").tgz  . && \
echo Pack binaries done, the path of package: $RDP_PROJ/package/rdp_mysql_${MYSQL_VERSION}.$(date "+%Y%m%d").tgz


;;

comm)

#make & install rdp-comm
cd $RDP_PROJ/comm/ && mkdir -p build && \
cd $RDP_PROJ/comm/build && cmake .. -DRDP_DEPS=$RDP_DEPS -DCMAKE_INSTALL_PREFIX=$RDP_DEPS/rdp-comm && make -j$CPU_NUM && make install && cd -;

;;

topic_translate)

cd $RDP_PROJ/tools/user_topic_trans/ && mkdir -p build && \
cd $RDP_PROJ/tools/user_topic_trans/build && cmake .. -DRDP_DEPS=$RDP_DEPS && make -j$CPU_NUM && cd -;

;;

fulltopic_check)                                                                
                                                                                 
cd $RDP_PROJ/tools/fulltopic_check/src/ && make clean && make && \
cd -;

;;

zk)                                                                
                                                                                 
cd $RDP_PROJ/tools/zk/ && make clean && make && \
cd -;

;;

generate_package)

cd $RDP_PROJ/tools/generate_package/ && cmake . && make && \
cd -;

esac

if [ $? -ne 0 ]; then
  echo "Something is wrong, please take a check"
  exit 2
fi
