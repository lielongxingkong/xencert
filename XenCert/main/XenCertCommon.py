#!/usr/bin/python
#
# Copyright (C) Citrix Systems Inc.
#
# This program is free software; you can redistribute it and/or modify 
# it under the terms of the GNU Lesser General Public License as published 
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

"""Manual Xen Certification script"""

from optparse import OptionParser
import StorageHandler
import VmHandler
from Logging import Print

storage_type = "storage type (iscsi, hba, nfs, fs, vm)"

# argument format:
#  keyword
#  text
#  white space
#  default value
#  short form of option
#  log form of option
__nfs_args__ = [
    ["server",          "server name/IP addr", " : ", None,        "required", "-n", ""   ],
    ["serverpath",      "exported path", " : ", None,        "required", "-e", ""     ] ]

__hba_args__ = [
    ["adapters",       "comma separated list of HBAs to test against", " : ", None,        "optional", "-a", ""   ] ]

__fs_args__ = [
    ["device",          "block device to create file system", " : ", None,        "required", "-d", ""   ],
    ["mountpoint",          "mount point path", " : ", None,        "optional", "-m", ""   ],
    ["fs",       "file system type to create", " : ", 'ocfs2',        "optional", "-f", ""   ] ]

__iscsi_args__ = [
    ["target",          "comma separated list of Target names/IP addresses", " : ", None,        "required", "-t", ""      ],
    ["targetIQN",       "comma separated list of target IQNs OR \"*\"", " : ", None,        "required", "-q", ""      ],
    ["SCSIid",        "SCSIid to use for datastore creation",                  " : ", '',          "optional", "-s", ""    ],
    ["chapuser",        "username for CHAP", " : ", '',        "optional", "-x", ""    ],
    ["chappasswd",      "password for CHAP", " : ", '',        "optional", "-w", ""    ] ]


__vm_args__ = [
    ["name",          "name for virtual machine", " : ", None,        "optional", "-N", ""   ],
    ["rootDisk",      "root virtual disk for virtual machine", " : ", None,        "required", "-R", ""   ],
    ["after",       "action after vm test ", " : ", None,        "optional", "-A", ""   ],
    ["path",       "path to create vm", " : ", None,        "optional", "-p", ""   ],
    ["storeOn",       "disk to create new data storage", " : ", None,        "optional", "-o", ""   ] ]

__common__ = [    
    ["functional", "perform functional tests",                          " : ", None, "optional", "-F", ""],
    ["multipath", "perform multipath configuration verification tests", " : ", None, "optional", "-M", ""],
    ["data", "perform data verification tests",                         " : ", None, "optional", "-D", ""],
    ["help",    "show this help message and exit",                                  " : ", None,        "optional", "-h", "" ]]

__commonparams__ = [
    ["storage_type",    storage_type,                     " : ", None, "required", "-b", ""],
    ["pathHandlerUtil", "absolute path to admin provided callout utility which blocks/unblocks a list of paths, path related information should be provided with the -i option below",
                                                                                    " : ", None, "optional", "-u", ""],
    ["pathInfo", "pass-through string used to pass data to the callout utility above, for e.g. login credentials etc. This string is passed as-is to the callout utility. ",
                                                                                    " : ", None, "optional", "-i", ""],
    ["count", "count of iterations to perform in case of multipathing failover testing",
                                                                                    " : ", None, "optional", "-g", ""],
    ["type",      "type whether skip whole disk check", " : ", 'q',        "optional", "-T", ""  ] ]

def parse_args(version_string):
    """Parses the command line arguments"""
    
    opt = OptionParser("usage: %prog [arguments seen below]",
            version=version_string,
           add_help_option=False)
    
    for element in __nfs_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
    
    for element in __hba_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
   
    for element in __fs_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])

    for element in __iscsi_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
        
    for element in __vm_args__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])

    for element in __commonparams__:
        opt.add_option(element[5], element[6],
                       default=element[3],
                       help=element[1],
                       dest=element[0])
    
    for element in __common__:
        opt.add_option(element[5], element[6],
                       action="store_true",
                       default=element[3],
                       help=element[1],
                       dest=element[0])

    return opt.parse_args()

def store_configuration(g_storage_conf, options):
    """Stores the command line arguments in a class"""

    g_storage_conf["storage_type"] = options.storage_type
    try:
        g_storage_conf["slavehostname"] = options.slavehostname
    except:
        pass

def valid_arguments(options, g_storage_conf):
    """ validate arguments """
    if not options.storage_type in ["hba", "nfs", "iscsi", "fs", "vm"]:
        Print("Error: storage type (hba, nfs, fs, iscsi or vm) is required")
        return 0

    for element in __commonparams__:
        if not getattr(options, element[0]):
            if element[4] == "required":
                Print("Error: %s argument (%s: %s) for storage type %s" \
                       % (element[4], element[5], element[1], options.storage_type))
                return 0
            else:
                g_storage_conf[element[0]] = "" 
        value = getattr(options, element[0])
        g_storage_conf[element[0]] = value

    if options.storage_type == "nfs":
        subargs = __nfs_args__
    elif options.storage_type == "hba":
        subargs = __hba_args__
    elif options.storage_type == "fs":
        subargs = __fs_args__
    elif options.storage_type == "iscsi":
        subargs = __iscsi_args__
    elif options.storage_type == "vm":
        subargs = __vm_args__

    for element in subargs:
        if not getattr(options, element[0]):
            if element[4] == "required":
                Print("Error: %s argument (%s: %s) for storage type %s" \
                       % (element[4], element[5], element[1], options.storage_type))
                DisplayUsage(options.storage_type)
                return 0
            else:
                g_storage_conf[element[0]] = "" 
        value = getattr(options, element[0])
        g_storage_conf[element[0]] = value
        
    return 1

def GetStorageHandler(g_storage_conf):
    # Factory method to instantiate the correct handler
    if g_storage_conf["storage_type"] == "iscsi":
        return StorageHandler.StorageHandlerISCSI(g_storage_conf)
    
    if g_storage_conf["storage_type"] == "hba":
        return StorageHandler.StorageHandlerHBA(g_storage_conf)
        
    if g_storage_conf["storage_type"] == "nfs":
        return StorageHandler.StorageHandlerNFS(g_storage_conf)
    
    if g_storage_conf["storage_type"] == "fs":
        return StorageHandler.StorageHandlerFS(g_storage_conf)

    if g_storage_conf["storage_type"] == "vm":
        return VmHandler.VmHandler(g_storage_conf)

    return None

def DisplayCommonOptions():
    Print("usage: XenCert [arguments seen below] \n\
\n\
Common options:\n")
    for item in __common__:
        printHelpItem(item)
    
def DisplayiSCSIOptions():
    Print(" Storage type iscsi:\n")
    for item in __iscsi_args__:
        printHelpItem(item)
 
def DisplayNfsOptions():
    Print(" Storage type nfs:\n")
    for item in __nfs_args__:
        printHelpItem(item)
  
def DisplayHBAOptions():
    Print(" Storage type hba:\n")
    for item in __hba_args__:
        printHelpItem(item)    

def DisplayFsOptions():
    Print(" Storage type fs:\n")
    for item in __fs_args__:
        printHelpItem(item)    
  
def DisplayVmOptions():
    Print(" Storage type vm:\n")
    for item in __vm_args__:
        printHelpItem(item)    

def DisplayTestSpecificOptions():
    Print("Test specific options:")
    Print("Multipathing test options (-m above):\n")
    for item in __commonparams__:
        printHelpItem(item)

def DisplayStorageSpecificUsage(storage_type):
    if storage_type == 'iscsi':
        DisplayiSCSIOptions()
    elif storage_type == 'nfs':
        DisplayNfsOptions()
    elif storage_type == 'hba':
        DisplayHBAOptions()
    elif storage_type == 'fs':
        DisplayFsOptions()
    elif storage_type == 'vm':
        DisplayVmOptions()
    elif storage_type == None:
        DisplayiSCSIOptions()
        Print("")
        DisplayNfsOptions()
        Print("")
        DisplayHBAOptions()        
        Print("")
        DisplayFsOptions()        
        Print("")
        DisplayVmOptions()
     
def DisplayUsage(storage_type = None):
    DisplayCommonOptions();
    Print("\nStorage specific options:\n")
    DisplayStorageSpecificUsage(storage_type)
    Print("")
    DisplayTestSpecificOptions();

def printHelpItem(item):
    Print(" %s %-20s\t[%s] %s" % (item[5], item[0], item[4], item[1]))
    
