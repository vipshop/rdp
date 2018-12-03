# Schema Store初始化步骤
1. 请确保目录结构如下
```
├── bin
│   └── rdp_syncer
└── scripts
.    ├── init_schema_store.sh
.    ├── mysqlpump
.    ├── remove_schema_snapshot.sh
.    ├── replay_schema_snapshot.sh
.    ├── reset_schema_store.sh
.    └── take_schema_snapshot.sh
```
初始化过程中需要用到reset_schema_store.sh和init_schema_store.sh, 其他脚本将会在Schema Store运行过程中用到.

1. 建立Schema Store本身需要的表结构
```
cd scripts
./reset_schema_store.sh --host=192.168.0.xxx --port=3306 --user=root --passwd=root123
```
其中192.168.0.xxx是Schema Store的地址.


1. 从用户MySQL导入表结构到Schema Store
```
cd scripts
./init_schema_store.sh --src-host=192.168.0.xxx --src-port=3306 --src-user=root --src-passwd=root123  
--dst-host=192.168.0.xxx --dst-port=3306 --dst-user=root --dst-passwd=root123
```
其中192.168.0.xxx是用户MySQL的地址, 192.168.0.xxx是Schema Store的地址.

> 此命令会输出一个gtid_set, 请以该gtid_set作为RDP的checkpoint.
