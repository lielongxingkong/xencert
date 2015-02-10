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

"""Storage handler classes for various storage drivers"""
import sys
import StorageHandlerUtil
from Logging import Print, PrintR, PrintY, PrintB, PrintG, DebugCmd, DebugCmdArray
from Logging import PrintOnSameLine
from Logging import XenCertPrint
from Logging import displayOperationStatus
from StorageHandlerUtil import DISKDATATEST
from StorageHandlerUtil import CreateImg, RemoveImg, WriteDataToImg, VerifyDataOnImg
import scsiutil, iscsilib
import util
import glob
from threading import Thread
import time
import os
import random
import nfs
import commands
import ISCSI
from lvhdutil import VG_LOCATION,VG_PREFIX
from lvutil import MDVOLUME_NAME, ensurePathExists, remove, rename
from FileSystem import MOUNT_BASE, EXT4, XFS, OCFS2

retValIO = 0
timeTaken = '' 
bytesCopied = ''
speedOfCopy = ''
pathsFailed = False
failoverTime = 0

# simple tracer
def report(predicate, condition):
    if predicate != condition:
        Print("Condition Failed, check SMlog")

# Hardcoded time limit for Functional tests in hours
timeLimitFunctional = 4

class TimedDeviceIO(Thread):
    def __init__(self, device):
        Thread.__init__(self)
        self.device = device

    def run(self):
        # Sleep for a period of time before checking for any incomplete snapshots to clean.
        devicename = '/dev/' + self.device
        ddOutFile = 'of=' + devicename
        XenCertPrint("Now copy data from /dev/zero to this device and record the time taken to copy it." )
        cmd = ['dd', 'if=/dev/zero', ddOutFile, 'bs=1M', 'count=1', 'oflag=direct']
        DebugCmdArray(cmd)
        try:
            global retValIO
            global bytesCopied
            global timeTaken
            global speedOfCopy
            retValIO = 0
            timeTaken = '' 
            bytesCopied = ''
            speedOfCopy = ''

            (retValIO, stdout, stderr) = util.doexec(cmd,'')
            if retValIO != 0:
                raise Exception("Disk IO failed for device: %s." % self.device)
            list = stderr.split('\n')
            
            bytesCopied = list[2].split(',')[0]
            timeTaken = list[2].split(',')[1]
            speedOfCopy = list[2].split(',')[2]

            XenCertPrint("The IO test returned rc: %s stdout: %s, stderr: %s" % (retValIO, stdout, list))
        except Exception, e:
                XenCertPrint("Could not write through the allocated disk space on test disk, please check the storage configuration manually. Exception: %s" % str(e))

class WaitForFailover(Thread):
    def __init__(self, session, scsiid, activePaths, noOfPaths):
        Thread.__init__(self)        
        self.scsiid = scsiid
        self.activePaths = activePaths
        self.noOfPaths = noOfPaths

    def run(self):
        # Here wait for the expected number of paths to fail.        
        active = 0
        global pathsFailed
        global failoverTime
        pathsFailed = False
        failoverTime = 0        
        while not pathsFailed and failoverTime < 50:
            try:
                (retVal, listPathConfigNew) = StorageHandlerUtil.get_path_status(self.scsiid, True)
                if self.noOfPaths == ((int)(self.activePaths) - len(listPathConfigNew)):
                    pathsFailed = True
                time.sleep(1)
                failoverTime += 1                
            except Exception, e:                
                raise Exception(e)
            
class StorageHandler:
    def __init__(self, storage_conf):
        XenCertPrint("Reached Storagehandler constructor")
        self.storage_conf = storage_conf
        self.sm_config = {}
    
    def MPConfigVerificationTests(self):
        try:
            retVal =True
            checkPoint = 0
            totalCheckPoints = 3
            iterationCount = 100
            
            #TODO
            # Check if block unblock callouts have been defined. Else display an error and fail this test
            #if self.storage_conf['pathHandlerUtil'] == None:                 
            #    raise Exception("Path handler util not specified for multipathing tests.")
                
            #if not os.path.exists(self.storage_conf['pathHandlerUtil']):                 
            #    raise Exception("Path handler util specified for multipathing tests does not exist!")
            
            #if self.storage_conf['storage_type'] == 'hba' and self.storage_conf['pathInfo'] == None: 
            #    raise Exception("Path related information not specified for storage type hba.")
            
            if self.storage_conf['count'] != None:
                iterationCount = int(self.storage_conf['count']) + 1
            
            #1. Enable host Multipathing
            #TODO Record resource to release later.
            device_config = self.Create()

            if not StorageHandlerUtil.update_multipath_conf():
                raise Exception("   - Failed to update multipath config.")
            else:
                checkPoint += 1

            Print("MULTIPATH TESTING")
            #Print("MULTIPATH AUTOMATED PATH FAILOVER TESTING")

            if not self.GetPathStatus(device_config):
                raise Exception("   - Failed to get and display path status.")
            else:
                checkPoint += 1

            PrintB(">> Starting Multipath Device IO test")
            #Print(">> Starting Random Path Block and Restore Iteration test")
            #Print("   This test will choose a random selection of upto (n -1) paths ")
            #Print("   of a total of n to block, and verify that the IO continues")
            #Print("   i.e. the correct paths are detected as failed, within 50 seconds.")
            #Print("   The test then verifies that after unblocking the path, it is ")
            #Print("   restored within 2 minutes.\n\n")
            #Print("   Path Connectivity Details")
            self.DisplayPathStatus()

            for (mpDevname, listPathConfig) in self.mapPathConfig.items():
                totalCheckPoints += 1
                # make sure there are at least 2 paths for the multipath tests to make any sense.
                if len(listPathConfig) < 2:
                    PrintY("FATAL! At least 2 paths are required for multipath failover testing, please configure your storage accordingly.")
                    
                # Calculate the number of active paths here
                self.initialActivePaths = 0
                for tuple in listPathConfig:
                    if tuple[1] == 'active':
                        self.initialActivePaths += 1
                # Now testing failure times for the paths.  
                global retValIO
                global timeTaken
                global bytesCopied
                global speedOfCopy
                Print("")
                Print("Iteration 1 for Multipath Device %s:\n" % mpDevname)
                Print(" -> No manual blocking of paths.")
                s = TimedDeviceIO(mpDevname)
                s.start()
                s.join()
                
                if retValIO != 0:
                    displayOperationStatus(False)
                    raise Exception(" IO tests failed for device: %s" % mpDevname)
                
                initialDataCopyTime = float(timeTaken.split()[0])
                if initialDataCopyTime > 3:
                    displayOperationStatus(False, timeTaken)
                    Print("    - The initial data copy is too slow at %s" % timeTaken )
                    dataCopyTooSlow = True
                else:
                    Print("    - IO test passed. Time: %s. Data: %s. Throughput: %s" % (timeTaken, '1MB', speedOfCopy))
                    displayOperationStatus(True)

                checkPoint += 1

            #comment multipath failover test
            '''
            if len(listPathConfig) > 1:                
                for i in range(2, iterationCount):
                    maxTimeTaken = 0
                    throughputForMaxTime = ''
                    totalCheckPoints += 2
                    Print("Iteration %d:\n" % i)                                    
                    if not self.RandomlyFailPaths():                                            
                        raise Exception("Failed to block paths.")
                    
                    XenCertPrint("Dev Path Config = '%s', no of Blocked switch Paths = '%s'" % (listPathConfig, self.noOfPaths))

                    # Fail path calculation needs to be done only in case of hba SRs
                    if "blockunblockhbapaths" in \
                            self.storage_conf['pathHandlerUtil'].split('/')[-1]:
                        #Calculate the number of devices to be found after the path block
                        devicesToFail = (len(listPathConfig)/self.noOfTotalPaths) * self.noOfPaths
                        XenCertPrint("Expected devices to fail: %s" % devicesToFail)
                    else:
                        devicesToFail = self.noOfPaths

                    s = WaitForFailover(self.session, device_config['SCSIid'], len(listPathConfig), devicesToFail)
                    s.start()
                    
                    while s.isAlive():
                        timeTaken = 0
                        s1 = TimedDeviceIO(self.session.xenapi.VBD.get_device(vbd_ref))                    
                        s1.start()
                        s1.join()
                        
                        if retValIO != 0:                        
                            displayOperationStatus(False)
                            raise Exception("    - IO test failed for device %s." % self.session.xenapi.VBD.get_device(vbd_ref))
                        else:
                            XenCertPrint("    - IO test passed. Time: %s. Data: %s. Throughput: %s." % (timeTaken, '1MB', speedOfCopy))
                            
                        if timeTaken > maxTimeTaken:
                            maxTimeTaken = timeTaken
                            throughputForMaxTime = speedOfCopy
                    
                    if pathsFailed:
                        Print("    - Paths failover time: %s seconds" % failoverTime)
                        Print("    - Maximum IO completion time: %s. Data: %s. Throughput: %s" % (maxTimeTaken, '1MB', throughputForMaxTime))
                        displayOperationStatus(True)
                        checkPoint += 1
                    else:
                        displayOperationStatus(False)
                        self.BlockUnblockPaths(False, self.storage_conf['pathHandlerUtil'], self.noOfPaths, self.blockedpathinfo)
                        raise Exception("    - Paths did not failover within expected time.")
                    
                    self.BlockUnblockPaths(False, self.storage_conf['pathHandlerUtil'], self.noOfPaths, self.blockedpathinfo)
                    Print(" -> Unblocking paths, waiting for restoration.")
                    count = 0
                    pathsMatch = False
                    while not pathsMatch and count < 120:
                        pathsMatch = self.DoNewPathsMatch(device_config)
                        time.sleep(1)
                        count += 1
                        
                    if not pathsMatch:
                        displayOperationStatus(False, "> 2 mins")
                        retVal = False 
                        raise Exception("The path restoration took more than 2 mins.")
                    else:
                        displayOperationStatus(True, " " + str(count) + " seconds")
                        checkPoint += 1
            '''

            Print("- Test succeeded.")
 
        except Exception, e:
            Print("- There was an exception while performing multipathing configuration tests.")
            Print("  Exception: %s" % str(e))
            displayOperationStatus(False)
            retVal = False

        try:
            # Try cleaning up here

            # If multipath was enabled by us, disable it, else continue.
            #TODO
                
            checkPoint += 1
                
        except Exception, e:
            Print("- Could not cleanup the objects created during testing. Please destroy the objects manually. Exception: %s" % str(e))
            displayOperationStatus(False)

        XenCertPrint("Checkpoints: %d, totalCheckPoints: %s" % (checkPoint, totalCheckPoints))
        return (retVal, checkPoint, totalCheckPoints)
        
    def DataPerformanceTests(self):
        XenCertPrint("Reached StorageHandler DataPerformanceTests")

    # blockOrUnblock = True for block, False for unblock
    def BlockUnblockPaths(self, blockOrUnblock, script, noOfPaths, passthrough):
        try:
            stdout = ''
            if blockOrUnblock:
                cmd = [os.path.join(os.getcwd(), script), 'block', str(noOfPaths), passthrough]
            else:
                cmd = [os.path.join(os.getcwd(), script), 'unblock', str(noOfPaths), passthrough]
            
            (rc, stdout, stderr) = util.doexec(cmd,'')

            XenCertPrint("The path block/unblock utility returned rc: %s stdout: '%s', stderr: '%s'" % (rc, stdout, stderr))
            if rc != 0:                
                raise Exception("   - The path block/unblock utility returned an error: %s. Please block/unblock the paths %s manually." % (stderr, passthrough))
            return stdout
        except Exception, e:            
            raise e        
    
    def __del__(self):
        XenCertPrint("Reached Storagehandler destructor")
        
    def Create(self):
        # This class specific function will create an SR of the required type and return the required parameters.
        XenCertPrint("Reached StorageHandler Create")
        
    def DoNewPathsMatch(self, device_config):
        try:
            # get new config
            (retVal, listPathConfigNew) = StorageHandlerUtil.get_path_status(device_config['SCSIid'])
            XenCertPrint("listpathconfig: %s" % self.listPathConfig)
            XenCertPrint("listpathconfigNew: %s" % listPathConfigNew)
            if not retVal:                
                raise Exception("     - Failed to get path status information for SCSI Id: %s" % device_config['SCSIid'])
            
            # Find new number of active paths
            newActivePaths = 0
            for tuple in listPathConfigNew:
                if tuple[1] == 'active':
                    newActivePaths += 1
            
            if newActivePaths < self.initialActivePaths:                            
                    return False
            return True
        except Exception, e:
            XenCertPrint("Failed to match new paths with old paths.")
            return False

    def RawDiskPerformance(self, diskpath):
        size = 1000
        cmd = ['dd', 'if=/dev/zero', 'of=%s' % diskpath, 'bs=1M', 'count=%s' % size, 'conv=nocreat', 'oflag=direct']
        DebugCmdArray(cmd)
        time1 = time.time()
        util.pread(cmd)
        duration = time.time() - time1
        PrintB('>' * 20 + '  Start of Performance Test Result  ' + '>' * 20)
        PrintB('\tTest1 : %sMB Data Writen in %.2f seconds, Throuput %.2f MB/s' % (size, duration, size/duration))
        PrintB('>' * 20 + '  End of Performance Test Result  ' + '>' * 20)

    def RawDiskFunctional(self, diskpath):
        cmd = ['dd', 'if=/dev/zero', 'of=%s' % diskpath, 'bs=1M', 'count=1', 'conv=nocreat', 'oflag=direct']
        DebugCmdArray(cmd)
        util.pread(cmd)

    def FileSystemPerformance(self, testfile):
        size = 1000
        cmd = ['dd', 'if=/dev/zero', 'of=%s' % testfile, 'bs=1M', 'count=%s' % size, 'oflag=direct']
        DebugCmdArray(cmd)
        time1 = time.time()
        (rc, stdout, stderr) = util.doexec(cmd, '')
        duration = time.time() - time1
        PrintB('>' * 20 + '  Start of Performance Test Result  ' + '>' * 20)
        PrintB('\tTest1 : %sMB Data Writen in %.2f seconds, Throuput %.2f MB/s' % (size, duration, size/duration))
        PrintB('>' * 20 + '  End of Performance Test Result  ' + '>' * 20)
        return (rc, stdout, stderr)

    def FileSystemFunctional(self, testfile):
        cmd = ['dd', 'if=/dev/zero', 'of=%s' % testfile, 'bs=1M', 'count=1', 'oflag=direct']
        DebugCmdArray(cmd)
        return util.doexec(cmd, '')
     
class StorageHandlerISCSI(StorageHandler):
    def __init__(self, storage_conf):
        XenCertPrint("Reached StorageHandlerISCSI constructor")
        self.device_config = {}
        self.device_config['target'] = storage_conf['target']
        self.device_config['targetIQN'] = storage_conf['targetIQN']
        self.device_config['SCSIid'] = storage_conf['SCSIid']
        self.iqn = storage_conf['targetIQN']
        StorageHandler.__init__(self, storage_conf)        
    
    def setMdPath(self):
        # come up with the management volume name
        # add SR name_label
        self.mdpath = os.path.join(VG_LOCATION, VG_PREFIX + self.sr_uuid)
        self.mdpath = os.path.join(self.mdpath, MDVOLUME_NAME)

    def MPConfigVerificationTests(self):
        PrintR("iSCSI storage multipath test not supported yet.")
        return (False, 0, 1)
        
    def removeMGTVolume(self):
        login = False
        try:
            try:
                # logon to the iscsi session so LVs come up
                iscsilib.login(self.storage_conf['target'], self.storage_conf['targetIQN'], '', '')
                login = True
                
                # Allow the LVs to appear
                time.sleep(5)

                # remove the MGT volume
                remove(self.mdpath)
            except Exception, e:
                raise Exception("Failed to remove the management volume, error: %s" % str(e))
        finally:
            if login:
                # logout of the iscsi session
                iscsilib.logout(self.storage_conf['target'], self.storage_conf['targetIQN'])
                
    def Create(self, device_config = {}):
        device_config['target'] = self.storage_conf['target']
        if len(self.iqn.split(',')) > 1:
            device_config['targetIQN'] = '*'
        else:
            device_config['targetIQN'] = self.iqn
        if self.storage_conf['chapuser']!= None and self.storage_conf['chappasswd'] != None:
            device_config['chapuser'] = self.storage_conf['chapuser']
            device_config['chappassword'] = self.storage_conf['chappasswd']

        listPortalIQNs = self.ISCSIDiscoveryTargets(device_config['target'].split(','), device_config['targetIQN'].split(','))
        if len(listPortalIQNs) == 0:
            raise Exception("   - No Target found!")
        portal, iqn = listPortalIQNs[0]
        self.ISCSILogin(portal, iqn)
        #TODO
        #logoutlist.append((portal,iqn))
        lunToScsi = StorageHandlerUtil.get_lun_scsiid_devicename_mapping(iqn, portal)
        if len(lunToScsi.keys()) == 0:
            raise Exception("   - No LUNs found!")
        device_config['SCSIid'] = lunToScsi["0"][0]

        return device_config

    def GetPathStatus(self, device_config):
        # Query DM-multipath status, reporting a) Path checker b) Path Priority handler c) Number of paths d) distribution of active vs passive paths
        try:
            self.mapIPToHost = StorageHandlerUtil._init_adapters()      
            XenCertPrint("The IP to host id map is: %s" % self.mapIPToHost) 
            
            (retVal, configMap) = StorageHandlerUtil.GetConfig(device_config['SCSIid'])
            if not retVal:                
                raise Exception("   - Failed to get SCSI config information for SCSI Id: %s" % device_config['SCSIid'])

            XenCertPrint("The config map extracted from scsi_id %s is %s" % (device_config['SCSIid'], configMap))
            
            # Get path_checker and priority handler for this device.
            (retVal, mpath_config) = StorageHandlerUtil.parse_config(configMap['ID_VENDOR'], configMap['ID_MODEL'])
            if not retVal:                
                raise Exception("   - Failed to get multipathd config information for vendor: %s and product: %s" % (configMap['ID_VENDOR'], configMap['ID_MODEL']))
            XenCertPrint("The mpath config extracted from multipathd is %s" % mpath_config)

            Print(">> Multipathd enabled for %s, %s with the following config" % (configMap['ID_VENDOR'], configMap['ID_MODEL']))
            Print("   please confirm that these settings are optimal:")
            Print("     device {")
            for key in mpath_config:
                Print("             %s %s" % (key, mpath_config[key]))

            Print("     }")
 
            (retVal, listPathConfig, mpDevname) = StorageHandlerUtil.get_path_status(device_config['SCSIid'])
            if not retVal or mpDevname == None:
                raise Exception("Failed to get path status information for SCSI Id: %s" % device_config['SCSIid'])
            XenCertPrint("The path status extracted from multipathd is %s" % listPathConfig)
            self.mapPathConfig[mpDevname] = listPathConfig
            
            return True
        except Exception, e:
            Print("   - Failed to get path status for device_config: %s. Exception: %s" % (device_config, str(e)))
            return False            

    def DisplayPathStatus(self):
        for (mpDevname, listPathConfig) in self.mapPathConfig.items():
            PrintY("       Multipath Mapping Device on %s" % mpDevname)
            Print("       %-15s %-15s %-25s %-15s" % ('IP address', 'HBTL','Path DM status','Path status')            )
            for item in listPathConfig:
                Print("       %-15s %-15s %-25s %-15s" % (StorageHandlerUtil.findIPAddress(self.mapIPToHost, item[0]), item[0], item[1], item[2]))
            
    def RandomlyFailPaths(self):
        try:
            self.noOfPaths = random.randint(1, len(self.listPathConfig) -1 )   
            self.blockedpathinfo = ''
            self.paths = ''
            for item in self.listPathConfig: 
                ip = StorageHandlerUtil.findIPAddress(self.mapIPToHost, item[0])
                self.paths += ip + ','
                       
            self.paths = self.paths.rstrip(',')
            (self.blockedpathinfo) = self.BlockUnblockPaths(True, self.storage_conf['pathHandlerUtil'], self.noOfPaths, self.paths)
            PrintOnSameLine(" -> Blocking %d paths (%s)\n" % (self.noOfPaths, self.blockedpathinfo))
            return True                    
        except Exception, e:
            raise e

    def ISCSIDiscoveryTarget(self, target):
        try:
            return iscsilib.discovery(target, ISCSI.DEFAULT_PORT, self.storage_conf['chapuser'], self.storage_conf['chappasswd'])                                        
        except Exception, e:
            Print("Exception discovering iscsi target: %s, exception: %s" % (target, str(e)))
            displayOperationStatus(False)
            raise

    def ISCSIDiscoveryTargets(self, targetList, iqnFilter):
        wildcard = False
        if len(iqnFilter) == 1 and iqnFilter[0]=='*':
            wildcard = True

        listPortalIQNs = []
        for target in targetList:
            iscsi_map = self.ISCSIDiscoveryTarget(target)
            # Create a list of portal IQN combinations.
            for record in iscsi_map:
                for iqn in iqnFilter:
                    if record[2] == iqn or wildcard:
                        try:
                            listPortalIQNs.index((record[0], record[2]))
                        except Exception, e:
                            listPortalIQNs.append((record[0], record[2]))
                            break
        return listPortalIQNs

    def ISCSILogin(self, portal, iqn):
        # Login to this IQN, portal combination
        iscsilib.login(portal, iqn, self.storage_conf['chapuser'], self.storage_conf['chappasswd'])
        XenCertPrint("Logged on to the target.")
        # Now test the target
        iscsilib._checkTGT(portal)
        XenCertPrint("Checked the target.")

    def ISCSILogout(self, portal, iqn):
        try:
            XenCertPrint("Logging out of the session: %s, %s" % (portal, iqn))
            iscsilib.logout(portal, iqn) 
        except Exception, e:
            Print("- Logout failed for the combination %s, %s, but it may not have been logged on so ignore the failure." % (portal, iqn))
            Print("  Exception: %s" % str(e))

    def ISCSIPerformance(self, diskpath):
        self.RawDiskPerformance(diskpath)

    def ISCSIFunctional(self, diskpath):
        self.RawDiskFunctional(diskpath)

    def FunctionalTests(self):
        return self.FunctionalOrPerformanceTests('Func')

    def DataPerformanceTests(self):
        return self.FunctionalOrPerformanceTests('Perf')

    def FunctionalOrPerformanceTests(self, type):

        if type not in ('Perf', 'Func'):
            PrintR("Unsupport Selection %s, Perf and Func only" % type)
            raise Exception("Unsupport Selection %s, Perf and Func only" % type)

        logoutlist = []
        retVal = True
        checkPoint = 0
        totalCheckPoints = 4
        timeForIOTestsInSec = 0
        totalSizeInMiB = 0
        quickTest = False

        try:
            # Take device-config parameters and initialise data path layer.        
            Print("INITIALIZING SCSI DATA PATH LAYER ")
            
            if self.storage_conf['type'] == 'q' or self.storage_conf == 'quick':
                quickTest = True
            if type == 'Perf':
                quickTest = True
                
            iqns = self.storage_conf['targetIQN'].split(',')
            targets = self.storage_conf['target'].split(',')
            listPortalIQNs = self.ISCSIDiscoveryTargets(targets, iqns)
            displayOperationStatus(True)
            checkPoint += 1

            # Now traverse through this multimap and for each IQN
            # Connect to all available portals in turn and verify that
            Print("DISCOVERING ADVERTISED SESSION TARGETS")
            Print("   %-70s %-20s" % ('IQN', 'Session Target'))
            for (portal, iqn) in listPortalIQNs:
                Print("   %-70s %-20s" % (iqn, portal))
        
            displayOperationStatus(True)
            checkPoint += 1

            Print("REPORT LUNS EXPOSED")
            Print(">> This test logs on to all the advertised target and IQN combinations")
            Print("   and discovers the LUNs exposed by each including information")
            Print("   like the LUN ID, SCSI ID and the size of each LUN.")
            Print("   This test also verifies that all the sessions from the same IQN ")
            Print("   expose the same number of LUNs and the same LUNs.")
            Print("")
            # Create a map of the following format
            # SCSIid -> (portal, iqn, device) tuple list            
            scsiToTupleMap = {}
            # and one of the following format
            # iqn -> [SCSI IDS]
            # for each portal below, check if iqn is in the map
            # if yes check if the SCSI Ids match, else report error
            # if iqn not in map add iqn and list of SCSI IDs.
            iqnToScsiList = {}
            firstPortal = True
            for (portal, iqn) in listPortalIQNs:
                try:
                    scsilist = []
                    self.ISCSILogin(portal, iqn)
                    logoutlist.append((portal,iqn))                        

                    lunToScsi = StorageHandlerUtil.get_lun_scsiid_devicename_mapping(iqn, portal)
                    if len(lunToScsi.keys()) == 0:
                        raise Exception("   - No LUNs found!")
                        
                    XenCertPrint("The portal %s and the iqn %s yielded the following LUNs on discovery:" % (portal, iqn))
                    mapDeviceToHBTL = scsiutil.cacheSCSIidentifiers()
                    XenCertPrint("The mapDeviceToHBTL is %s" % mapDeviceToHBTL)
                          
                    if firstPortal:
                        Print("     %-23s\t%-4s\t%-34s\t%-10s" % ('PORTAL', 'LUN', 'SCSI-ID', 'Size(MiB)'))
                        firstPortal = False
                    for key in lunToScsi.keys():
                        # Find the HBTL for this lun
                        scsilist.append(lunToScsi[key][0])
                        HBTL = mapDeviceToHBTL[lunToScsi[key][1]]
                        HBTL_id = HBTL[1] + ":" + HBTL[2] + ":" + HBTL[3] + ":" + HBTL[4]
                        filepath = '/sys/class/scsi_device/' + HBTL_id + '/device/block/*/size'

                        # For clearwater version, the file path is device/block:*/size
                        filelist = glob.glob(filepath)
                        if not filelist:
                            filepath = '/sys/class/scsi_device/' + HBTL_id + '/device/block:*/size'
                            filelist = glob.glob(filepath)

                        XenCertPrint("The filepath is: %s" % filepath)
                        XenCertPrint("The HBTL_id is %s. The filelist is: %s" % (HBTL_id, filelist))
                        sectors = util.get_single_entry(filelist[0])
                        size = int(sectors) * 512 / 1024 / 1024
                        Print("     %-23s\t%-4s\t%-34s\t%-10s" % (portal, key, lunToScsi[key][0], size))

                        devname = lunToScsi[key][1]
                        if os.path.realpath(util.getrootdev()) != devname and not quickTest:
                            timeForIOTestsInSec += StorageHandlerUtil.FindDiskDataTestEstimate(devname, size)

                        if scsiToTupleMap.has_key(lunToScsi[key][0]):
                            scsiToTupleMap[lunToScsi[key][0]].append(( portal, iqn, lunToScsi[key][1]))
                        else:
                            scsiToTupleMap[lunToScsi[key][0]] = [( portal, iqn, lunToScsi[key][1])]
                        
                        totalSizeInMiB += size                                                   
                except Exception, e:
                    Print("     ERROR: No LUNs reported by portal %s for iqn %s. Exception: %s" % (portal, iqn, str(e)))
                    XenCertPrint("     ERROR: No LUNs reported by portal %s for iqn %s." % (portal, iqn))
                    raise Exception("     ERROR: No LUNs reported by portal %s for iqn %s." % (portal, iqn))
                
                if iqnToScsiList.has_key(iqn):
                    XenCertPrint("Reference scsilist: %s, current scsilist: %s" % (iqnToScsiList[iqn], scsilist))
                    if iqnToScsiList[iqn].sort() != scsilist.sort():
                        raise Exception("     ERROR: LUNs reported by portal %s for iqn %s do not match LUNs reported by other portals of the same IQN." % (portal, iqn))
                else:
                    iqnToScsiList[iqn] = scsilist
                        
            displayOperationStatus(True)
            checkPoint += 1

            Print("DISK IO TESTS")
            Print(">> This tests execute a disk IO test against each available LUN to verify ")
            Print("   that they are writeable and there is no apparent disk corruption.")
            Print("   the tests attempt to write to the LUN over each available path and")
            Print("   reports the number of writable paths to each LUN.")

            if not quickTest:                            
                seconds = timeForIOTestsInSec
                minutes = 0
                hrs = 0
                XenCertPrint("Total estimated time for the disk IO tests in seconds: %d" % timeForIOTestsInSec)
                if timeForIOTestsInSec > 60:
                    minutes = timeForIOTestsInSec/60
                    seconds = int(timeForIOTestsInSec - (minutes * 60))
                    if minutes > 60:
                        hrs = int(minutes/60)
                        minutes = int(minutes - (hrs * 60))
                
                if hrs > timeLimitFunctional or hrs == timeLimitFunctional and minutes > 0:
                    raise Exception("The disk IO tests will take more than %s hours, please restrict the total disk sizes above to %d GiB." % (timeLimitFunctional, (timeLimitFunctional*60*60*totalSizeInMiB)/timeForIOTestsInSec))
                    
                if hrs > 0:
                    Print("   APPROXIMATE RUN TIME: %s hours, %s minutes, %s seconds." % (hrs, minutes, seconds))
                elif minutes > 0:
                    Print("   APPROXIMATE RUN TIME: %s minutes, %s seconds." % (minutes, seconds))
                elif seconds > 0:
                    Print("   APPROXIMATE RUN TIME: %s seconds." % seconds)

            Print("   START TIME: %s " % (time.asctime(time.localtime())))
            Print("")
            firstPortal = True
            lunsMatch = True
            for key in scsiToTupleMap.keys():                                
                try:                    
                    totalCheckPoints += 1
                    Print("     - Testing LUN with SCSI ID %-30s" % key)
                    
                    pathNo = 0
                    pathPassed = 0
                    rootDevice = False
                    for tuple in scsiToTupleMap[key]:                        
                        rootDevice = False
                        # If this is a root device then skip IO tests for this device.
                        if os.path.realpath(util.getrootdev()) == tuple[2]:
                            PrintR("     -> Skipping IO tests on device %s, as it is the root device." % tuple[2])
                            rootDevice = True
                            continue
                        
                        pathNo += 1
        
                        # Execute a disk IO test against each path to the LUN to verify that it is writeable
                        # and there is no apparent disk corruption
                        PrintOnSameLine("        Path num: %d. Device: %s" % (pathNo, tuple[2]))
                        try:
                            # First write a small chunk on the device to make sure it works                    
                            XenCertPrint("First write a small chunk on the device %s to make sure it works." % tuple[2])

                            if type == 'Func':
                                self.ISCSIFunctional(tuple[2])
                            elif type == 'Perf':
                                self.ISCSIPerformance(tuple[2])

                            if not quickTest:                            
                                cmd = [DISKDATATEST, 'write', '1', tuple[2]]
                                XenCertPrint("The command to be fired is: %s" % cmd)
                                DebugCmdArray(cmd)
                                util.pread(cmd)
                                
                                cmd = [DISKDATATEST, 'verify', '1', tuple[2]]
                                XenCertPrint("The command to be fired is: %s" % cmd)
                                DebugCmdArray(cmd)
                                util.pread(cmd)
                                
                            XenCertPrint("Device %s passed the disk IO test. " % tuple[2])
                            pathPassed += 1
                            Print("")
                            displayOperationStatus(True)
                            
                        except Exception, e:  
                            Print("        Exception: %s" % str(e))
                            displayOperationStatus(False)
                            XenCertPrint("Device %s failed the disk IO test. Please check if the disk is writable." % tuple[2] )
                        
                    if pathPassed == 0 and not rootDevice:
                        displayOperationStatus(False)
                        raise Exception("     - LUN with SCSI ID %-30s. Failed the IO test, none of the paths were writable." % key)                        
                    else:
                        Print("        SCSI ID: %s Total paths: %d. Writable paths: %d." % (key, len(scsiToTupleMap[key]), pathPassed))
                        displayOperationStatus(True)
                        checkPoint += 1                            
                                
                except Exception, e:                    
                    raise Exception("   - Testing failed while testing devices with SCSI ID: %s." % key)
                
            Print("   END TIME: %s " % (time.asctime(time.localtime())))
            
            checkPoint += 1
        
        except Exception, e:
            Print("- Functional testing failed due to an exception.")
            Print("- Exception: %s"  % str(e))
            retVal = False
            
         # Logout of all the sessions in the logout list
        for (portal,iqn) in logoutlist:
            self.ISCSILogout(portal, iqn)

        XenCertPrint("Checkpoints: %d, totalCheckPoints: %s" % (checkPoint, totalCheckPoints))
        XenCertPrint("Leaving StorageHandlerISCSI FunctionalTests")

        return (retVal, checkPoint, totalCheckPoints)
    
    def __del__(self):
        XenCertPrint("Reached StorageHandlerISCSI destructor")
        StorageHandler.__del__(self)
        
class StorageHandlerHBA(StorageHandler):
    def __init__(self, storage_conf):
        XenCertPrint("Reached StorageHandlerHBA constructor")
        StorageHandler.__init__(self, storage_conf)

    def Create(self):
        device_config = {}
        device_config['adapters'] = self.storage_conf['adapters']

        listSCSIId = []
        (retVal, listAdapters, listSCSIId) = StorageHandlerUtil.GetHBAInformation(device_config, nolocal=True)
        if not retVal:                
            raise Exception("   - Failed to get available HBA information on the host.")
        if len(listSCSIId) == 0:                
            raise Exception("   - Failed to get available LUNs on the host.")

        device_config['SCSIid'] = ','.join(listSCSIId)
        return device_config

    def GetPathStatus(self, device_config):
        # Query DM-multipath status, reporting a) Path checker b) Path Priority handler c) Number of paths d) distribution of active vs passive paths
        try:            
            self.mapPathConfig = {}
            for SCSIid in device_config['SCSIid'].split(','):
                (retVal, configMap) = StorageHandlerUtil.GetConfig(SCSIid)
                if not retVal:                
                    raise Exception("   - Failed to get SCSI config information for SCSI Id: %s" % SCSIid)

                XenCertPrint("The config map extracted from scsi_id %s is %s" % (SCSIid, configMap))
                
                # Get path_checker and priority handler for this device.
                (retVal, mpath_config) = StorageHandlerUtil.parse_config(configMap['ID_VENDOR'], configMap['ID_MODEL'])
                if not retVal:
                    raise Exception("   - Failed to get multipathd config information for vendor: %s and product: %s" % (configMap['ID_VENDOR'], configMap['ID_MODEL']))
                    
                XenCertPrint("The mpath config extracted from multipathd is %s" % mpath_config)

                PrintY(">> Multipathd enabled for LUN %s, %s, %s with the following config:" % (SCSIid, configMap['ID_VENDOR'], configMap['ID_MODEL']))
                Print("     device {")
                for key in mpath_config:
                    Print("             %s %s" % (key, mpath_config[key]))

                Print("     }")
 
                (retVal, listPathConfig, mpDevname) = StorageHandlerUtil.get_path_status(SCSIid)
                if not retVal or mpDevname == None:                
                    raise Exception("Failed to get path status information for SCSI Id: %s" % SCSIid)
                XenCertPrint("The path status extracted from multipathd is %s" % listPathConfig)
                self.mapPathConfig[mpDevname] = listPathConfig
            
            return True
        except Exception, e:
            Print("   - Failed to get path status for device_config: %s. Exception: %s" % (device_config, str(e)))
            return False            

    def DisplayPathStatus(self):
        for (mpDevname, listPathConfig) in self.mapPathConfig.items():
            PrintY("       Multipath Mapping Device on %s" % mpDevname)
            Print("       %-15s %-25s %-15s" % ('HBTL','Path DM status','Path status')            )
            for item in listPathConfig:
                Print("       %-15s %-25s %-15s" % (item[0], item[1], item[2]))
            
    def RandomlyFailPaths(self):
        try:
            self.blockedpathinfo = ''
            self.noOfPaths = 0
            self.noOfTotalPaths = 0
            scriptReturn = self.BlockUnblockPaths(True, self.storage_conf['pathHandlerUtil'], self.noOfPaths, self.storage_conf['pathInfo'])
            blockedAndfull = scriptReturn.split('::')[1]
            self.noOfPaths = int(blockedAndfull.split(',')[0])
            self.noOfTotalPaths = int(blockedAndfull.split(',')[1])
            XenCertPrint("No of paths which should fail is %s out of total %s" % \
                                            (self.noOfPaths, self.noOfTotalPaths))
            self.blockedpathinfo = scriptReturn.split('::')[0]
            PrintOnSameLine(" -> Blocking paths (%s)\n" % self.blockedpathinfo)
            return True
        except Exception, e:            
            raise e

    def HBAPerformance(self, diskpath):
        self.RawDiskPerformance(diskpath)

    def HBAFunctional(self, diskpath):
        self.RawDiskFunctional(diskpath)

    def FunctionalTests(self):
        return self.FunctionalOrPerformanceTests('Func')

    def DataPerformanceTests(self):
        return self.FunctionalOrPerformanceTests('Perf')

    def FunctionalOrPerformanceTests(self, type='Func'):

        if type not in ('Perf', 'Func'):
            PrintR("Unsupport Selection %s, Perf and Func only" % type)
            raise Exception("Unsupport Selection %s, Perf and Func only" % type)

        retVal = True
        checkPoint = 0
        totalCheckPoints = 3
        timeForIOTestsInSec = 0
        totalSizeInMiB = 0
        quickTest = False

        if self.storage_conf['type'] == 'q' or self.storage_conf == 'quick':
            quickTest = True
        if type == 'Perf':
            quickTest = True
         
        try:
            # 1. Report the FC Host Adapters detected and the status of each physical port
            # Run a probe on the host with type lvmohba, parse the xml output and extract the HBAs advertised
            Print("DISCOVERING AVAILABLE HARDWARE HBAS")
            (retVal, listMaps, scsilist) = StorageHandlerUtil.GetHBAInformation(self.storage_conf)
            if not retVal:                
                raise Exception("   - Failed to get available HBA information on the host.")
            else:
                XenCertPrint("Got HBA information: %s and SCSI ID list: %s" % (listMaps, scsilist))
           
            if len(listMaps) == 0:                    
                     raise Exception("   - No hardware HBAs found!")

            checkPoint += 1
            first = True

            for map in listMaps:                
                if first:
                    for key in map.keys():
                        PrintOnSameLine("%-15s\t" % key)
                    PrintOnSameLine("\n")
                    first = False

                for key in map.keys(): 
                    PrintOnSameLine("%-15s\t" % map[key])
                PrintOnSameLine("\n")

            displayOperationStatus(True)
            checkPoint += 1 
                
            # 2. Report the number of LUNs and the disk geometry for verification by user
            # take each host id and look into /dev/disk/by-scsibus/*-<host-id>*
            # extract the SCSI ID from each such entries, make sure all have same
            # number of entries and the SCSI IDs are the same.
            # display SCSI IDs and luns for device for each host id. 
            Print("REPORT LUNS EXPOSED PER HOST")
            Print(">> This test discovers the LUNs exposed by each host id including information")
            Print("   like the HBTL, SCSI ID and the size of each LUN.")
            Print("   The test also ensures that all host ids ")
            Print("   expose the same number of LUNs and the same LUNs.")
            Print("")
            first = True
            hostIdToLunList = {}
            # map from SCSI id -> list of devices
            scsiToTupleMap = {}
            for map in listMaps:
                try:
                    (rVal, listLunInfo) = StorageHandlerUtil.GetLunInformation(map['id'])
                    if not rVal:                                                    
                        raise Exception("Failed to get LUN information for host id: %s" % map['id'])
                    else:
                        XenCertPrint("Got LUN information for host id %s as %s" % (map['id'], listLunInfo))
                        hostIdToLunList[map['id']] = listLunInfo

                    Print("     The luns discovered for host id %s: " % map['id'])
                    mapDeviceToHBTL = scsiutil.cacheSCSIidentifiers()
                    XenCertPrint("The mapDeviceToHBTL is %s" % mapDeviceToHBTL)

                    if first and len(listLunInfo) > 0:
                        Print("     %-4s\t%-34s\t%-20s\t%-10s" % ('LUN', 'SCSI-ID', 'Device', 'Size(MiB)'))
                        first = False
                                                    
                    for lun in listLunInfo:
                        # Find the HBTL for this lun
                        HBTL = mapDeviceToHBTL[lun['device']]
                        HBTL_id = HBTL[1] + ":" + HBTL[2] + ":" + HBTL[3] + ":" + HBTL[4]
                        filepath = '/sys/class/scsi_device/' + HBTL_id + '/device/block/*/size'

                        # For clearwater version, the file path is device/block:*/size
                        filelist = glob.glob(filepath)
                        if not filelist:
                            filepath = '/sys/class/scsi_device/' + HBTL_id + '/device/block:*/size'
                            filelist = glob.glob(filepath)

                        XenCertPrint("The filepath is: %s" % filepath)
                        XenCertPrint("The HBTL_id is %s. The filelist is: %s" % (HBTL_id, filelist))
                        sectors = util.get_single_entry(filelist[0])
                        size = int(sectors) * 512 / 1024 / 1024
                        Print("     %-4s\t%-34s\t%-20s\t%-10s" % (lun['id'], lun['SCSIid'], lun['device'], size))

                        devname = lun['device']
                        if os.path.realpath(util.getrootdev()) != devname and not quickTest:
                            timeForIOTestsInSec += StorageHandlerUtil.FindDiskDataTestEstimate(devname, size)

                        if scsiToTupleMap.has_key(lun['SCSIid']):
                            scsiToTupleMap[lun['SCSIid']].append(lun['device'])
                        else:
                            scsiToTupleMap[lun['SCSIid']] = [lun['device']]
                        
                        totalSizeInMiB += size           

                except Exception, e:
                    Print("     EXCEPTION: No LUNs reported for host id %s." % map['id'])
                    continue
                displayOperationStatus(True)

            checkPoint += 1

            # 3. Execute a disk IO test against each LUN to verify that they are writeable and there is no apparent disk corruption            
            Print("DISK IO TESTS")
            Print(">> This tests execute a disk IO test against each available LUN to verify ")
            Print("   that they are writeable and there is no apparent disk corruption.")
            Print("   the tests attempt to write to the LUN over each available path and")
            Print("   reports the number of writable paths to each LUN.")
            if not quickTest:
                seconds = timeForIOTestsInSec
                minutes = 0
                hrs = 0
                XenCertPrint("Total estimated time for the disk IO tests in seconds: %d" % timeForIOTestsInSec)
                if timeForIOTestsInSec > 60:
                    minutes = int(timeForIOTestsInSec/60)
                    seconds = int(timeForIOTestsInSec - (minutes * 60))
                    if minutes > 60:
                        hrs = int(minutes/60)
                        minutes = int(minutes - (hrs * 60))
                
                if hrs > timeLimitFunctional or hrs == timeLimitFunctional and minutes > 0:
                    raise Exception("The disk IO tests will take more than %s hours, please restrict the total disk sizes above to %d GiB." % (timeLimitFunctional, (timeLimitFunctional*60*60*totalSizeInMiB)/timeForIOTestsInSec))                

                if hrs > 0:
                    Print("   APPROXIMATE RUN TIME: %s hours, %s minutes, %s seconds." % (hrs, minutes, seconds))
                elif minutes > 0:
                    Print("   APPROXIMATE RUN TIME: %s minutes, %s seconds." % (minutes, seconds))
                elif seconds > 0:
                    Print("   APPROXIMATE RUN TIME: %s seconds." % seconds)            
            
            Print("   START TIME: %s " % (time.asctime(time.localtime())))
            Print("")            
            totalCheckPoints += 1
            for key in scsiToTupleMap.keys():
                try:
                    totalCheckPoints += 1
                    Print("     - Testing LUN with SCSI ID %-30s" % key)

                    pathNo = 0
                    pathPassed = 0
                    rootDevice = False
                    for device in scsiToTupleMap[key]:
                        rootDevice = False
                        # If this is a root device then skip IO tests for this device.
                        if os.path.realpath(util.getrootdev()) == device:
                            PrintR("     -> Skipping IO tests on device %s, as it is the root device." % device)
                            rootDevice = True
                            continue

                        pathNo += 1
                        
                        # Execute a disk IO test against each path to the LUN to verify that it is writeable
                        # and there is no apparent disk corruption
                        PrintOnSameLine("        Path num: %d. Device: %s" % (pathNo, device))
                        try:
                            # First write a small chunk on the device to make sure it works
                            XenCertPrint("First write a small chunk on the device %s to make sure it works." % device)

                            if type == 'Func':
                                self.HBAFunctional(device)
                            elif type == 'Perf':
                                self.HBAPerformance(device)

                            if not quickTest:                            
                                cmd = [DISKDATATEST, 'write', '1', device]
                                XenCertPrint("The command to be fired is: %s" % cmd)
                                util.pread(cmd)
                                
                                cmd = [DISKDATATEST, 'verify', '1', device]
                                XenCertPrint("The command to be fired is: %s" % cmd)
                                util.pread(cmd)
                                
                            XenCertPrint("Device %s passed the disk IO test. " % device)
                            pathPassed += 1
                            Print("")
                            displayOperationStatus(True)

                        except Exception, e:
                            Print("        Exception: %s" % str(e))
                            displayOperationStatus(False)
                            XenCertPrint("Device %s failed the disk IO test. Please check if the disk is writable." % device )
                    if pathPassed == 0 and not rootDevice:
                        displayOperationStatus(False)
                        raise Exception("     - LUN with SCSI ID %-30s. Failed the IO test, none of the paths were writable." % key)                        
                    else:
                        Print("        SCSI ID: %s Total paths: %d. Writable paths: %d." % (key, len(scsiToTupleMap[key]), pathPassed))
                        displayOperationStatus(True)
                        checkPoint += 1

                except Exception, e:
                    raise Exception("   - Testing failed while testing devices with SCSI ID: %s." % key)

            Print("   END TIME: %s " % (time.asctime(time.localtime())))
            checkPoint += 1

        except Exception, e:
            Print("- Functional testing failed due to an exception.")
            Print("- Exception: %s"  % str(e))
            retVal = False
            
        XenCertPrint("Checkpoints: %d, totalCheckPoints: %s" % (checkPoint, totalCheckPoints))
        XenCertPrint("Leaving StorageHandlerHBA FunctionalTests")

        return (retVal, checkPoint, totalCheckPoints)

    def __del__(self):
        XenCertPrint("Reached StorageHandlerHBA destructor")
        StorageHandler.__del__(self)

class StorageHandlerNFS(StorageHandler):
    def __init__(self, storage_conf):
        XenCertPrint("Reached StorageHandlerNFS constructor")
        self.server = storage_conf['server']
        self.serverpath = storage_conf['serverpath']        
        StorageHandler.__init__(self, storage_conf)
        
    def __del__(self):
        XenCertPrint("Reached StorageHandlerNFS destructor")
        StorageHandler.__del__(self)
        
    def DisplayNFS(self):
        try:               
            cmd = [nfs.SHOWMOUNT_BIN, "--no-headers", "-e", self.storage_conf['server']]
            DebugCmdArray(cmd)
            list =  util.pread2(cmd).split('\n')
            if len(list) > 0:
                Print("   %-50s" % 'Exported Path')
            for val in list:
                if len(val.split()) > 0:
                    Print("   %-50s" % val.split()[0])
    
        except Exception, e:
            Print("   - Failed to display exported paths for server: %s. Exception: %s" % 
                                                         (self.storage_conf['server'], str(e)))
    	    raise e

    def MountNFS(self, mountpoint):
        try:
            util.makedirs(mountpoint, 755)                
            nfs.soft_mount(mountpoint, self.storage_conf['server'], self.storage_conf['serverpath'], 'tcp')
        except Exception, e:
            raise e

    def UmountNFS(self, mountpoint):
        nfs.unmount(mountpoint, True)

    def NFSPerformance(self, testfile):
        return self.FileSystemPerformance(testfile)

    def NFSFunctional(self, testfile):
        return self.FileSystemFunctional(testfile)

    def FunctionalTests(self):
        return self.FunctionalOrPerformanceTests('Func')

    def DataPerformanceTests(self):
        return self.FunctionalOrPerformanceTests('Perf')

    def FunctionalOrPerformanceTests(self, type='Func'):

        if type not in ('Perf', 'Func'):
            PrintR("Unsupport Selection %s, Perf and Func only" % type)
            raise Exception("Unsupport Selection %s, Perf and Func only" % type)

        retVal = True
        checkPoints = 0
        totalCheckPoints = 5
        testFileCreated = False
        testDirCreated = False
        mountCreated = False

        mountpoint = '/mnt/XenCertTest-' + commands.getoutput('uuidgen') 
        try:
            # 1. Display various exports from the server for verification by the user. 
            PrintY("DISCOVERING EXPORTS FROM THE SPECIFIED TARGET")
            Print(">> This test probes the specified NFS target and displays the ")
            Print(">> various paths exported for verification by the user. ")

            self.DisplayNFS()
            displayOperationStatus(True)
            checkPoints += 1

            # 2. Verify NFS target by mounting as local directory
            PrintY("VERIFY NFS TARGET PARAMETERS")
            Print(">> This test attempts to mount the export path specified ")
            Print(">> as a local directory. ")
            try:                
                self.MountNFS(mountpoint)
                mountCreated = True
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:                
                raise Exception("   - Failed to mount exported path: %s on server: %s, error: %s" % (self.storage_conf['server'], self.storage_conf['serverpath'], str(e)))       
            
            # 2. Create directory and execute Filesystem IO tests
            PrintY("CREATE DIRECTORY AND PERFORM FILESYSTEM IO TESTS.")
            Print(">> This test creates a directory on the locally mounted path above")
            Print(">> and performs some filesystem read write operations on the directory.")
            try:
                testdir = os.path.join(mountpoint, 'XenCertTestDir-%s' % commands.getoutput('uuidgen'))
                try:
                    os.mkdir(testdir, 755)
                except Exception,e:                    
                    raise Exception("Exception creating directory: %s" % str(e))
                testDirCreated = True

                testfile = os.path.join(testdir, 'XenCertNFSPerfTestFile-%s' % commands.getoutput('uuidgen'))
                if type == 'Func':
                    rc, stdout, stderr = self.NFSFunctional(testfile)
                elif type == 'Perf':
                    rc, stdout, stderr = self.NFSPerformance(testfile)

                testFileCreated = True
                if rc != 0:                    
                    raise Exception(stderr)
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:
                Print("   - Failed to perform filesystem IO tests.")
                raise e        
            
            # 3. Report Filesystem target space parameters for verification by user
            PrintY("REPORT FILESYSTEM TARGET SPACE PARAMETERS FOR VERIFICATION BY THE USER")
            try:
                Print("  - %-20s: %s" % ('Total space', util.get_fs_size(testdir)))
                Print("  - %-20s: %s" % ('Space utilization',util.get_fs_utilisation(testdir)))
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:
                Print("   - Failed to report filesystem space utilization parameters. " )
                raise e 
        except Exception, e:
            Print("   - Functional testing failed with error: %s" % str(e))
            retVal = False   

        # Now perform some cleanup here
        try:
            if testFileCreated:
                os.remove(testfile)
            if testDirCreated:
                os.rmdir(testdir)
            if mountCreated:
                self.UmountNFS(mountpoint)
            checkPoints += 1
        except Exception, e:
            Print("   - Failed to cleanup after NFS functional tests, please delete the following manually: %s, %s, %s. Exception: %s" % (testfile, testdir, mountpoint, str(e)))
            
        return (retVal, checkPoints, totalCheckPoints)   

    def MPConfigVerificationTests(self):
        PrintR("No multipath support for NFS storage.")
        return (False, 0, 1)
        
class StorageHandlerFS(StorageHandler):
    def __init__(self, storage_conf):
        XenCertPrint("Reached StorageHandlerFS constructor")
        self.device = storage_conf['device']
        self.mountpoint = storage_conf['mountpoint']
        if storage_conf['fs'] == 'ext4':
           self.fs = EXT4(self.device, path=self.mountpoint)
        elif storage_conf['fs'] == 'xfs':
           self.fs = XFS(self.device, path=self.mountpoint)
        elif storage_conf['fs'] == 'ocfs2':
           self.fs = OCFS2(self.device, path=self.mountpoint)
        else:
            raise Exception("Unsupport filesystem %s, ext4, xfs and ocfs2 only" % storage_conf['fs'])

        if os.path.realpath(util.getrootdev()) in self.device:
            raise Exception("FS test not support device %s, as it is the root device." % self.device)
        StorageHandler.__init__(self, storage_conf)
        
    def __del__(self):
        XenCertPrint("Reached StorageHandlerFS destructor")
        StorageHandler.__del__(self)
        
    def CreateFS(self):
        try:
            self.fs.create()
        except Exception, e:
            Print("   - Failed to create FS on device : %s. Exception: %s" % (self.device, str(e)))
    	    raise e

    def MountFS(self):
        try:
            self.fs.attach()
        except Exception, e:
            Print("   - Failed to mount device %s to %s : %s. Exception: %s" % (self.device, self.mountpoint, str(e)) )
            raise e

    def UmountFS(self):
        self.fs.detach()

    def FSPerformance(self, testfile):
        return self.FileSystemPerformance(testfile)

    def FSFunctional(self, testfile):
        return self.FileSystemFunctional(testfile)

    def FunctionalTests(self):
        return self.FunctionalOrPerformanceTests('Func')

    def DataPerformanceTests(self):
        return self.FunctionalOrPerformanceTests('Perf')

    def FunctionalOrPerformanceTests(self, type='Func'):

        if type not in ('Perf', 'Func'):
            PrintR("Unsupport Selection %s, Perf and Func only" % type)
            raise Exception("Unsupport Selection %s, Perf and Func only" % type)

        retVal = True
        checkPoints = 0
        totalCheckPoints = 5
        testFileCreated = False
        testDirCreated = False
        mountCreated = False

        try:
            # 1. Create FS
            PrintY("CREATE FS ON DEVICE")
            Print(">> This test create filesystem on device.")

            self.CreateFS()
            displayOperationStatus(True)
            checkPoints += 1

            # 2. Verify FS by mounting as local directory
            PrintY("VERIFY FS TARGET PARAMETERS")
            Print(">> This test attempts to mount the export path specified ")
            Print(">> as a local directory. ")
            try:
                self.MountFS()
                mountCreated = True
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:
                raise Exception("   - Failed to mount device %s on path %s, error: %s" % (self.device, self.mountpoint, str(e)))       
            
            # 2. Create directory and execute Filesystem IO tests
            PrintY("CREATE DIRECTORY AND PERFORM FILESYSTEM IO TESTS.")
            Print(">> This test creates a directory on the locally mounted path above")
            Print(">> and performs some filesystem read write operations on the directory.")
            try:
                testdir = os.path.join(self.fs.get_mountpoint(), 'XenCertTestDir-%s' % commands.getoutput('uuidgen'))
                try:
                    os.mkdir(testdir, 755)
                except Exception,e:
                    raise Exception("Exception creating directory: %s" % str(e))
                testDirCreated = True

                testfile = os.path.join(testdir, 'XenCertFSPerfTestFile-%s' % commands.getoutput('uuidgen'))
                if type == 'Func':
                    rc, stdout, stderr = self.FSFunctional(testfile)
                elif type == 'Perf':
                    rc, stdout, stderr = self.FSPerformance(testfile)

                testFileCreated = True
                if rc != 0:
                    raise Exception(stderr)
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:
                Print("   - Failed to perform filesystem IO tests.")
                raise e
            
            # 3. Report Filesystem target space parameters for verification by user
            PrintY("REPORT FILESYSTEM TARGET SPACE PARAMETERS FOR VERIFICATION BY THE USER")
            try:
                Print("  - %-20s: %s" % ('Total space', util.get_fs_size(testdir)))
                Print("  - %-20s: %s" % ('Space utilization',util.get_fs_utilisation(testdir)))
                displayOperationStatus(True)
                checkPoints += 1
            except Exception, e:
                Print("   - Failed to report filesystem space utilization parameters. " )
                raise e
        except Exception, e:
            Print("   - Functional testing failed with error: %s" % str(e))
            retVal = False

        # Now perform some cleanup here
        try:
            if testFileCreated:
                os.remove(testfile)
            if testDirCreated:
                os.rmdir(testdir)
            if mountCreated:
                self.UmountFS()
            checkPoints += 1
        except Exception, e:
            Print("   - Failed to cleanup after FS tests, please delete the following manually: %s, %s, %s. Exception: %s" % (testfile, testdir, self.mountpoint, str(e)))
            
        return (retVal, checkPoints, totalCheckPoints)

    def MPConfigVerificationTests(self):
        PrintR("No multipath support for FS storage.")
        return (False, 0, 1)
 
