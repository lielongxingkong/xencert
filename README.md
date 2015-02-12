#Inspur InCloud Sphere Storage Certification Kit
####*by Zhao Zhelong*  <zhaozhl@inspur.com>

---

##1. Test Categories

### 1.1 About Storage
+ **Functional Test**
+ **Performance Test**
+ **Multipath Test**

  |               |  FC   | SAS  | ISCSI |  NFS  |  RAID |  FS   |
  | ------------- | ----- | ---- | ----- | ----- | ----- | ----- |
  | multipath     | yes   | none | future| none  | none  | none  |
  | functional    | yes   | yes  | yes   | yes   | yes   | yes   |
  | performance   | yes   | yes  | yes   | yes   | yes   | yes   |
  | path failover | future| none | future| none  | none  | none  |

### 1.2 About Virtual Machine
+ **Functional Test**

  | power on | power off | ip/mac/passwd inject | vm check |
  | -------- | --------- | -------------------- | -------- |
  | yes      | yes       | next                 | next     |
  

##2. inspur-xencert command usage

###2.1 install
>rpm -ivh inspur-xencert-0.1-0.x86_64.rpm

###2.2 Usage
>inspur-xencert [arguments seen below]

###2.3 Common options:

+ -F
	+ perform **functional** tests for storage
+ -M
	+ perform **multipath** tests for storage
+ -D
	+ perform **performance** tests for storage
+ no arguments given
	+ perform **functional  multipath  performance** tests for storage
	+ perform **functional** test for vm when "**-b vm**" given
+ -h
	+ show this help message and exit  

###2.4 Storage specific options

####2.4.1 Storage type iscsi

+ -t target [required]
  + comma separated list of Target names/IP addresses 
+ -q targetIQN [required]
  + comma separated list of target IQNs OR "*" 
+ -x chapuser [optional]
  + username for CHAP 
+ -w chappasswd [optional]
  + password for CHAP

####2.4.2 Storage type nfs

+ -n server [required]
  + server name/IP addr
+ -e serverpath [required]
  + exported path

####2.4.3 Storage type hba

+ -a device [optional]
  + comma separated list of HBAs to test against

####2.4.4 Storage type fs

+ -d device [required]
  + block device to create file system
+ -m mountpoint [optional]
  + mount point path
+ -f fs [optional]
  + file system type ( xfs, ocfs2, ext4 ), ocfs2 default
  
###2.5 VM specific options

####2.5.1 VM test

+ -N name [optional]
  + name for virtual machine
+ -R rootDisk [required]
  + root virtual disk for virtual machine
+ -A after [optional]
  + action after vm test
    + "r" or "remove", purge vm and umount data storage
    + "p" or "permanent", not purge vm and not umount data storage
    + "c" or "clean", purge vm and not umount data storage
+ -p path [optional]
  + path to create vm
+ -o storeOn [optional]
  + disk to create new data storage
  
###2.6 Test specific options
+ -b test_type [required]
  + storage test type ( iscsi, hba, nfs, fs ) 
  + vm test ( vm ) 
  

>
**NOTE for ISCSI:**    If the target IQNs are specified as "\*" in the -q option above, ALL the LUNs accessible via the targets mentioned in -t will be accessed and **ERASED**. Please use the wildcard option **with the utmost care**.
    
>
**NOTE for HBA:**    If no adapters specified for the -a option above, ALL the disks on HBAs accessible will be **ERASED** except the OS root disk. Please use no -a option **with the utmost care**.

 
##3 Running on various storage types 
###3.1 Executing iSCSI tests

>inspur-xencert -b iscsi -t [IP1,IP2,...] -q [IQN1,IQN2,...] 

```
examples:
	inspur-xencert -b iscsi -t 100.1.8.100 -q iqn.2003-01.org.linux-iscsi.master.x8664:sn.2f29a244197d
	inspur-xencert -b iscsi -t 100.1.8.100 -q iqn.2003-01.org.linux-iscsi.master.x8664:sn.2f29a244197d -F
	inspur-xencert -b iscsi -t 100.1.8.100 -q iqn.2003-01.org.linux-iscsi.master.x8664:sn.2f29a244197d -D
	inspur-xencert -b iscsi -t 100.1.8.100 -q "*"
	inspur-xencert -b iscsi -t 100.1.8.100 -q "*" -F
	inspur-xencert -b iscsi -t 100.1.8.100 -q "*" -D
```

###3.2 Executing HBA tests


>inspur-xencert -b hba -a [adapter1,adapter2,...]

```
examples:
	inspur-xencert -b hba 
	inspur-xencert -b hba -F 
	inspur-xencert -b hba -D
	inspur-xencert -b hba -M
	inspur-xencert -b hba -a host0,host1
	inspur-xencert -b hba -a host0,host1 -F
	inspur-xencert -b hba -a host0,host1 -D
	inspur-xencert -b hba -a host0,host1 -M
```

>If no adapter is specified, the tests would be run against all the adapters with LUNs mapped to the server. 


###3.3 Executing NFS tests

>inspur-xencert -b nfs -n [server] -e [serverpath]

```
examples:
	inspur-xencert -b nfs -n 192.168.1.160 -e /Common/NFS1 
	inspur-xencert -b nfs -n 192.168.1.160 -e /Common/NFS1 -F
	inspur-xencert -b nfs -n 192.168.1.160 -e /Common/NFS1 -D
```

###3.4 Executing FS tests

>inspur-xencert -b fs -d [device] -m [mountpoint] -f [type]

```
examples:
	inspur-xencert -b fs -d /dev/sdb 
	inspur-xencert -b fs -d /dev/sdb -M 
	inspur-xencert -b fs -d /dev/sdb -D
	inspur-xencert -b fs -d /dev/sdb -m /mnt
	inspur-xencert -b fs -d /dev/sdb -m /mnt -f xfs
	inspur-xencert -b fs -d /dev/sdb -m /mnt -f ocfs2
	inspur-xencert -b fs -d /dev/sdb -m /mnt -f ocfs2 -F
	inspur-xencert -b fs -d /dev/sdb -m /mnt -f ocfs2 -D
```

##4 Running on virtual machines 
###4.1 Executing VM tests

>inspur-xencert -b vm -N [name] -R [rootdisk] -A [after] -p [path] -o [storeOn] 

```
examples:
	inspur-xencert -b vm -R /opt/centos7.img
	inspur-xencert -b vm -R /opt/centos7.img -N el7test
	inspur-xencert -b vm -R /opt/centos7.img -o /dev/sdb
	inspur-xencert -b vm -R /opt/centos7.img -o /dev/sdb -A p
	inspur-xencert -b vm -R /opt/centos7.img -o /dev/sdb -p /mnt -A p
	inspur-xencert -b vm -R /opt/centos7.img /mnt -A p
```

>
Note: "-A p" and "-A c" option is to continue using vm test root directory. It is useful performing a vm test suite.