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

"""Virtual machine handler classes"""
import sys, os
import uuid, commands
import util
from Logging import Print, PrintR, PrintY, PrintB, PrintG, DebugCmd, DebugCmdArray
from Logging import PrintOnSameLine
from Logging import XenCertPrint
from Logging import displayOperationStatus
from FileSystem import MOUNT_BASE, EXT4, XFS, OCFS2
from XenVM import XenVM, DEFAULT_BR_NAME

DEFAULT_PORT_NAME = 'eth1'

class VmHandler:
    def __init__(self, storage_conf):
        XenCertPrint("Reached Vmhandler constructor")

        self.name = str(uuid.uuid4()) if storage_conf['name'] == None else storage_conf['name']
        self.storeOn = storage_conf['storeOn']
        self.rootDisk = storage_conf['rootDisk']
        self.path = storage_conf['path']
        self.fs = None

        if storage_conf['after'] == 'r' or storage_conf['after'] == 'remove' or storage_conf['after'] == None: 
            self.purgeAfter = True
            self.umountAfter = True 
        elif storage_conf['after'] == 'p' or storage_conf['after'] == 'permanent': 
            self.purgeAfter = False
            self.umountAfter = False
        elif storage_conf['after'] == 'c' or storage_conf['after'] == 'clean': 
            self.purgeAfter = True
            self.umountAfter = False
        else:
            raise Exception("Unsupport after argument %s, c(clean) p(permanent) r(remove) only" % storage_conf['after'])

        if self.path != None and not os.path.exists(self.path):
            raise Exception("path %s for VM test does not exist!" % self.path)

        if self.storeOn != None:
            if not os.path.exists(self.storeOn):                 
                raise Exception("Device path %s for VM test does not exist!" % self.storeOn)

            if os.path.realpath(util.getrootdev()) in self.storeOn:
                raise Exception("VM test not support device %s, as it is the root device." % self.storeOn)

            self.fs = OCFS2(self.storeOn, path=self.path)
            self.path = self.fs.get_mountpoint()
        
        if self.path == None:
            self.path = MOUNT_BASE 

        if not os.path.exists(self.rootDisk):                 
            raise Exception("Virtual Disk path %s for VM test does not exist!" % self.rootDisk)

        self.storage_conf = storage_conf
        self.sm_config = {}

    def InitNetwork(self):
        #config bridege
        result = commands.getoutput("ovs-vsctl list-br")
        if not DEFAULT_BR_NAME in result.split('\n'):
            (rc, result) = commands.getstatusoutput("ovs-vsctl add-br %s" % DEFAULT_BR_NAME)
            if rc != 0: 
                raise Exception("add-br error when init network, do it manually. Message : %s " % result)
        #config port 
        (rc, result) = commands.getstatusoutput("ovs-vsctl port-to-br %s" % DEFAULT_PORT_NAME)
        if rc == 0:
            if result != DEFAULT_BR_NAME:
                #port in use by another bridge, delete it
                (rc, result) = commands.getstatusoutput("ovs-vsctl del-port %s" % DEFAULT_PORT_NAME)
                if rc != 0: 
                    raise Exception("del-port error when init network, do it manually. Message : %s " % result)
                (rc, result) = commands.getstatusoutput("ovs-vsctl add-port %s %s" % (DEFAULT_BR_NAME, DEFAULT_PORT_NAME))
                if rc != 0: 
                    raise Exception("add-port error when init network, do it manually. Message : %s " % result)
        else:
            (rc, result) = commands.getstatusoutput("ovs-vsctl add-port %s %s" % (DEFAULT_BR_NAME, DEFAULT_PORT_NAME))
            if rc != 0: 
                raise Exception("add-port error when init network, do it manually. Message : %s " % result)
        
    def FunctionalTests(self):
        retVal = True
        checkPoints = 0
        totalCheckPoints = 3
        mounted= False
        vmCreated = False

        vm = XenVM(self.name, path=self.path)

        try:
            # 1. Create FS if needed
            if self.fs != None:
                totalCheckPoints += 1
                PrintY("CREATE FS ON DEVICE")
                Print(">> This test create filesystem on device.")

                try: 
                    self.fs.create()
                    self.fs.attach()
                    mounted = True
                except Exception, e:
                    Print("   - Failed to create FS on device : %s. Exception: %s" % (self.storeOn, str(e)))
                    raise e

                displayOperationStatus(True)
                checkPoints += 1

            # 2. Create FS if needed
            PrintY("INIT NETWORK")
            Print(">> This test init vm network on device.")

            try: 
                self.InitNetwork()
            except Exception, e:
                Print("   - Failed to init network. Exception: %s" % str(e))
                raise e

            displayOperationStatus(True)
            checkPoints += 1

            # 3. Create directory and execute VM tests
            PrintY("CREATE DIRECTORY AND PERFORM VM TESTS.")
            Print(">> This test creates a directory to store virtual machine")
            Print(">> and performs vm tests.")
            try:
                vm.create()
                vm.import_rootdev(self.rootDisk)
                vm.start()
                vm.print_info()
                vmCreated = True
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:
                Print("   - Failed to perform VM tests.")
                raise e
            
        except Exception, e:
            Print("   - Functional testing failed with error: %s" % str(e))
            retVal = False

        # Now perform some cleanup here
        try:
            if vmCreated:
                vm.stop()
                if self.purgeAfter:
                    vm.remove()
            if mounted and self.umountAfter:
                self.fs.detach()
            checkPoints += 1
        except Exception, e:
            Print("   - Failed to cleanup after VM tests, please cleanup manually. Exception: %s" % str(e))
            
        return (retVal, checkPoints, totalCheckPoints)

