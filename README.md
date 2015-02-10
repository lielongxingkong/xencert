#Inspur InCloud Sphere Storage Certification Kit
####*by Zhao Zhelong*  <zhaozhl@inspur.com>

---

##1. Test Categories

+ **Functional Test**
+ **Performance Test**
+ **Multipath Test**

  |               |  FC   | SAS  | ISCSI |  NFS  |  RAID |
  | ------------- | ----- | ---- | ----- | ----- | ----- |
  | multipath     | yes   | none | future| none  | none  |
  | functional    | yes   | yes  | yes   | yes   | next  |
  | performance   | yes   | yes  | yes   | yes   | next  |
  | path failover | future| none | future| none  | none  |

  

##2. inspur-xencert command usage

###2.1 install
>rpm -ivh inspur-xencert-0.1-0.x86_64.rpm

###2.2 Usage
>inspur-xencert [arguments seen below]

###2.3 Common options:

+ -f
	+ perform **functional** tests
+ -m
	+ perform **multipath** tests 
+ -d
	+ perform **performance** tests
+ no arguments given
	+ perform **functional  multipath  performance** tests 
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

+ -a adapters [optional]
  + comma separated list of HBAs to test against

###2.5 Test specific options
+ -b storage_type [required]
  + storage type ( iscsi, hba, nfs ) 
  

>**NOTES:**    If the target IQNs are specified as "\*" in the -q option above, ALL the LUNs accessible via the targets mentioned in -t will be accessed and **ERASED**. Please use the wildcard option **with the utmost care**.

 
##3 Running on various storage types 
###3.1 Executing iSCSI tests

>inspur-xencert -b iscsi -t [IP1,IP2,...] -q [IQN1,IQN2,...] 

```
examples:
	inspur-xencert -b iscsi -t 100.1.8.100 -q iqn.2003-01.org.linux-iscsi.master.x8664:sn.2f29a244197d
	inspur-xencert -b iscsi -t 100.1.8.100 -q iqn.2003-01.org.linux-iscsi.master.x8664:sn.2f29a244197d -f
	inspur-xencert -b iscsi -t 100.1.8.100 -q iqn.2003-01.org.linux-iscsi.master.x8664:sn.2f29a244197d -d
	inspur-xencert -b iscsi -t 100.1.8.100 -q "*"
	inspur-xencert -b iscsi -t 100.1.8.100 -q "*" -f
	inspur-xencert -b iscsi -t 100.1.8.100 -q "*" -d
```

###3.2 Executing HBA tests


>inspur-xencert -b hba -a [adapter1,adapter2,...]

```
examples:
	inspur-xencert -b hba 
	inspur-xencert -b hba -f 
	inspur-xencert -b hba -d
	inspur-xencert -b hba -m
	inspur-xencert -b hba -a host0,host1
	inspur-xencert -b hba -a host0,host1 -f
	inspur-xencert -b hba -a host0,host1 -d
	inspur-xencert -b hba -a host0,host1 -m
```

>If no adapter is specified, the tests would be run against all the adapters with LUNs mapped to the server. 


###3.3 Executing NFS tests

>inspur-xencert -b nfs -n [server] -e [serverpath]

```
examples:
	inspur-xencert -b nfs 
	inspur-xencert -b nfs -n 192.168.1.160 -e /Common/NFS1
	inspur-xencert -b nfs -n 192.168.1.160 -e /Common/NFS1
	inspur-xencert -b nfs -n 192.168.1.160 -e /Common/NFS1
```
