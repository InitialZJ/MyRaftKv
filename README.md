# MyRaftKv

仿照[KVstorageBaseRaft-cpp](https://github.com/youngyangyang04/KVstorageBaseRaft-cpp)写的基于Raft的kv存储数据库，使用跳表进行数据存储，集群节点间的通信使用[MyMrpc](https://github.com/InitialZJ/MyMrpc)。

## 依赖库安装

首先安装一些必要组件
```bash
sudo apt install unzip autoconf libtool libboost-dev zlib1g-dev libboost-serialization-dev
```

### CMake

因为要使用c++20标准，所以需要高版本的CMake支持，我是用的版本是3.22。因为apt源没有这么高的版本，需要从其他地方安装，执行以下命令

```bash
sudo apt remove cmake
cd ~/Downloads
wget https://cmake.org/files/v3.22/cmake-3.22.0.tar.gz
tar -zxvf cmake-3.22.0.tar.gz
cd cmake-3.22.0
./configure
make -j8
sudo make install
sudo ln -s /usr/local/bin/cmake /usr/bin/cmake
```

### Protobuf

本项目使用的protobuf版本是3.11.0，在安装之前一定要先卸载干净原来的protobuf，不然会产生版本冲突。点击[链接](https://pan.baidu.com/s/1IbVEDgnoLkF5vEzIJOnRuA?pwd=5rmm )下载压缩包，复制到ubuntu，执行以下命令

```bash
unzip protobuf-master.zip
cd protobuf-master
./autogen.sh
./configure
make
sudo make install
sudo ldconfig
```

### Muduo

运行以下命令

```bash
git clone https://github.com/chenshuo/muduo.git
cd muduo
./build.sh install
```

会在上一级目录中生成`build`文件夹，里面有一个`release-install-cpp11`文件夹，记住这个文件夹的路径

## 使用方法

修改`MyRaftKv/CMakeLists.txt`，将第14行的路径换成你自己的，执行命令进行编译

```bash
mkdir build && cd build
cmake ..
make
```

进入`MyMrpc/bin`文件夹，可以看到生成了可执行文件`callerMain`和`raftCoreRun`，执行`./raftCoreRun -n 3 -f test.conf`启动数据库服务，可以看到Raft内部的运行情况。开启一个新终端，执行`./callerMain`调用`Get`和`Put`，可以看到方法调用成功，集群间节点运行正常。

![](https://raw.githubusercontent.com/InitialZJ/MarkdownPhotoes/main/res/Snipaste_2024-01-25_17-33-13.jpg)

![](https://raw.githubusercontent.com/InitialZJ/MarkdownPhotoes/main/res/Snipaste_2024-01-25_17-34-05.jpg)

![](https://raw.githubusercontent.com/InitialZJ/MarkdownPhotoes/main/res/Snipaste_2024-01-25_17-34-18.jpg)

![](https://raw.githubusercontent.com/InitialZJ/MarkdownPhotoes/main/res/Snipaste_2024-01-25_17-33-40.jpg)



