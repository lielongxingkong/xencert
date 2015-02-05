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
sys.path.insert(0, "../drivers")
sys.path.insert(0, "drivers")
import scsiutil
import util
import glob
import xml.dom.minidom
import lvutil, vhdutil
from lvhdutil import MSIZE
import iscsilib
import mpath_cli
import os
import re
import commands
import time
import mpath_dmp
import HBA

ISCSI_PROCNAME = "iscsi_tcp"
timeTaken = '' 
bytesCopied = ''
speedOfCopy = ''
logfile = None
logfilename = None
timeLimitControlInSec = 18000

MAX_TIMEOUT = 15

KiB = 1024
MiB = KiB * KiB
GiB = KiB * KiB * KiB

SECTOR_SIZE = 1 * GiB
CHAR_SEQ = "".join([chr(x) for x in range(256)])
CHAR_SEQ_REV = "".join([chr(x) for x in range(255, -1, -1)])
BUF_PATTERN = CHAR_SEQ + CHAR_SEQ
BUF_PATTERN_REV = CHAR_SEQ_REV + CHAR_SEQ_REV
BUF_ZEROS = "\0" * 512

DISKDATATEST = '/opt/xensource/debug/XenCert/diskdatatest'

multiPathDefaultsMap = { 'udev_dir':'/dev',
			    'polling_interval':'5',
			    'path_selector': "round-robin 0",
			    'path_grouping_policy':'failover',
			    'getuid_callout':"/lib/udev/scsi_id --whitelisted --device=/dev/%n",
			    'path_checker':'directio',
			    'rr_min_io':'1000',
			    'rr_weight':'uniform',
			    'failback':'manual',
			    'no_path_retry':'fail',
			    'user_friendly_names':'no' }

def PrintToLog(message):
    try:
	global logfile
	logfile.write(message)
        logfile.flush()
    except:
	pass
     
def color_it(message, color=None):
    color_map = {
        'r' : 31,
        'g' : 32,
        'y' : 33,
        'b' : 34,
        'w' : 37,
    }
    if color in color_map.keys():
	message = ('\033[1;%dm' % color_map[color]) + message 
	message = message + '\033[0m'
    return message
    
def PrintB(message):
    message = color_it(message, 'b')
    Print(message)

def PrintG(message):
    message = color_it(message, 'g')
    Print(message)

def PrintY(message):
    message = color_it(message, 'y')
    Print(message)

def PrintR(message):
    message = color_it(message, 'r')
    Print(message)

def DebugCmd(cmd):
    PrintR("# " + cmd)

def DebugCmdArray(cmd):
    if type(cmd) == type([]):
        DebugCmd(" ".join(cmd))
    elif type(cmd) == type(""):
        DebugCmd(cmd)
    else:
        DebugCmd(str(cmd))

def Print(message):
    # Print to the stdout and to a temp file.
    try:
	sys.stdout.write(message)
	sys.stdout.write('\n')
	global logfile
	logfile.write(message)
	logfile.write('\n')
        logfile.flush()
    except:
	pass

def PrintOnSameLine(message):
    # Print to the stdout and to a temp file.
    try:
	sys.stdout.write(message)
	global logfile
	logfile.write(message)	
        logfile.flush()
    except:
	pass
    
def InitLogging():
    global logfile
    global logfilename
    logfilename = os.path.join('/tmp', 'XenCert-' + commands.getoutput('uuidgen') + '.log')
    logfile = open(logfilename, 'a')

def UnInitLogging():
    global logfile
    logfile.close()
    
def GetLogFileName():
    global logfilename
    return logfilename

def XenCertPrint(message):
    util.SMlog("XenCert - " + message)

def displayOperationStatus(passOrFail, customValue = ''):
    if passOrFail:
        PrintG("                                                                                                   PASS [Completed%s]" % customValue)
    else:
        PrintR("                                                                                                   FAIL [%s]" % time.asctime(time.localtime()))

def _init_adapters():
    # Generate a list of active adapters
    ids = scsiutil._genHostList(ISCSI_PROCNAME)
    util.SMlog(ids)
    adapter = {}
    for host in ids:
        try:
            # For Backward compatibility 
            if hasattr(iscsilib, 'get_targetIQN'):
                targetIQN = iscsilib.get_targetIQN(host)
            else: 
                targetIQN = util.get_single_entry(glob.glob(\
                   '/sys/class/iscsi_host/host%s/device/session*/iscsi_session*/targetname' % host)[0])

            if hasattr(iscsilib, 'get_targetIP_and_port'):
                (addr, port) = iscsilib.get_targetIP_and_port(host)
            else:
                addr = util.get_single_entry(glob.glob(\
                   '/sys/class/iscsi_host/host%s/device/session*/connection*/iscsi_connection*/persistent_address' % host)[0])
                port = util.get_single_entry(glob.glob(\
                   '/sys/class/iscsi_host/host%s/device/session*/connection*/iscsi_connection*/persistent_port' % host)[0])

            entry = "%s:%s" % (addr,port)
            adapter[entry] = host
        except Exception, e:
            pass
    return adapter

def blockIP(ip):
    try:
	cmd = ['iptables', '-A', 'INPUT', '-s', ip, '-j', 'DROP']
        util.pread(cmd)
    except Exception, e:
        XenCertPrint("There was an exception in blocking ip: %s. Exception: %s" % (ip, str(e)))

def unblockIP(ip):
    try:
	cmd = ['iptables', '-D', 'INPUT', '-s', ip, '-j', 'DROP']
        util.pread(cmd)
    except Exception, e:
        XenCertPrint("There was an exception in unblocking ip: %s. Exception: %s" % (ip, str(e)))
   
def actualSRFreeSpace(size):
    num = (size - lvutil.LVM_SIZE_INCREMENT - 4096 - vhdutil.calcOverheadEmpty(MSIZE)) * vhdutil.VHD_BLOCK_SIZE
    den = 4096 + vhdutil.VHD_BLOCK_SIZE

    return num/den

def GetConfig(scsiid):
    try:
	retVal = True
	configMap = {}
	device = scsiutil._genReverseSCSIidmap(scsiid)[0]
	XenCertPrint("GetConfig - device: %s" % device)
	cmd = ["scsi_id", "-u", "-g", "-x", "-d", device]
	ret = util.pread2(cmd)
	XenCertPrint("GetConfig - scsi_if output: %s" % ret)
	for tuple in ret.split('\n'):
	    if tuple.find('=') != -1:
		configMap[tuple.split('=')[0]] = tuple.split('=')[1]

    except Exception, e:
	XenCertPrint("There was an exception getting SCSI device config. Exception: %s" % str(e))
	retVal = False

    return (retVal, configMap)

def findIPAddress(mapIPToHost, HBTL):
    try:
	ip = ''
	for key in mapIPToHost.keys():
	    if mapIPToHost[key] == HBTL.split(':')[0]:
		ip = key.split(':')[0]
		break
    except Exception, e:
	XenCertPrint("There was an exception in finding IP address for the HBTL: %s. Exception: %s" % (HBTL, str(e)))
    return ip

# The returned structure are a list of portals, and a list of SCSIIds for the specified IQN. 
def GetListPortalScsiIdForIqn(session, server, targetIqn, chapUser = None, chapPassword = None):
    try:
	listPortal = []
	listSCSIId= []
	device_config = {}
	device_config['target'] = server
	if chapUser != None and chapPassword != None:
	    device_config['chapuser'] = chapUser
	    device_config['chappassword'] = chapPassword

	try:
	    session.xenapi.SR.probe(util.get_localhost_uuid(session), device_config, 'lvmoiscsi')
	except Exception, e:
	    XenCertPrint("Got the probe data as: %s" % str(e))
	    
	# Now extract the IQN list from this data.
	try:
	    # the target may not return any IQNs
	    # so prepare for it
	    items = str(e).split(',')
	    xmlstr = ''
	    for i in range(3,len(items)):
		xmlstr += items[i]
		xmlstr += ','
	    
	    #xmlstr = str(e).split(',')[3]
	    xmlstr = xmlstr.strip(',')
	    xmlstr = xmlstr.lstrip()
	    xmlstr = xmlstr.lstrip('\'')
	    xmlstr = xmlstr.rstrip()
	    xmlstr = xmlstr.rstrip('\]')
	    xmlstr = xmlstr.rstrip('\'')
	    xmlstr = xmlstr.replace('\\n', '')
	    xmlstr = xmlstr.replace('\\t', '')		
	    XenCertPrint("Got the probe xml as: %s" % xmlstr)
	    dom = xml.dom.minidom.parseString(xmlstr)
	    TgtList = dom.getElementsByTagName("TGT")		
	    for tgt in TgtList:
		iqn = None
		portal = None
		for node in tgt.childNodes:
		    if node.nodeName == 'TargetIQN':
			iqn = node.firstChild.nodeValue
		
		    if node.nodeName == 'IPAddress':
			portal = node.firstChild.nodeValue

		XenCertPrint("Got iqn: %s, portal: %s" % (iqn, portal))
		XenCertPrint("The target IQN is: %s" % targetIqn)
		if iqn == '*':
		    continue
		for targetiqn in targetIqn.split(','):
		    if iqn == targetiqn:
			listPortal.append(portal)
			break
	    
	    XenCertPrint("The portal list at the end of the iteration is: %s" % listPortal)
	except Exception, e:
	    raise Exception("The target %s did not return any IQNs on probe. Exception: %s" % (server, str(e)))
		
	#  Now probe again with each IQN in turn.
	for iqn in targetIqn.split(','):
	    try:
		device_config['targetIQN'] = iqn
		XenCertPrint("Probing with device config: %s" % device_config)
		session.xenapi.SR.probe(util.get_localhost_uuid(session), device_config, 'lvmoiscsi')
	    except Exception, e:
		XenCertPrint("Got the probe data as: %s" % str(e))
    
	    # Now extract the SCSI ID list from this data.
	    try:
		# If there are no LUNs exposed, the probe data can be an empty xml
		# so be prepared for it
		items = str(e).split(',')
		xmlstr = ''
		for i in range(3,len(items)):
		    xmlstr += items[i]
		    xmlstr += ','
		#xmlstr = str(e).split(',')[3]
		xmlstr = xmlstr.strip(',')
		xmlstr = xmlstr.lstrip()
		xmlstr = xmlstr.lstrip('\'')
		xmlstr = xmlstr.rstrip()
		xmlstr = xmlstr.rstrip('\]')
		xmlstr = xmlstr.rstrip('\'')
		xmlstr = xmlstr.replace('\\n', '')
		xmlstr = xmlstr.replace('\\t', '')
		XenCertPrint("Got the probe xml as: %s" % xmlstr)
		dom = xml.dom.minidom.parseString(xmlstr)
		scsiIdObjList = dom.getElementsByTagName("SCSIid")                
		for scsiIdObj in scsiIdObjList:
		    listSCSIId.append(scsiIdObj.firstChild.nodeValue)
			
	    except Exception, e:
		XenCertPrint("The IQN: %s did not return any SCSI IDs on probe. Exception: %s" % (iqn, str(e)))
		    
	    XenCertPrint("Got the SCSIId list for iqn %s as %s" % (iqn, listSCSIId))
	    
	     
    except Exception, e: 
	XenCertPrint("There was an exception in GetListPortalScsiIdForIqn. Exception: %s" % str(e))
	raise Exception(str(e))
	
    
    XenCertPrint("GetListPortalScsiIdForIqn - returning PortalList: %s." % listPortal)  
    XenCertPrint("GetListPortalScsiIdForIqn - returning SCSIIdList: %s." % listSCSIId)  
    return (listPortal, listSCSIId)

def extract_xml_from_exception(e):
    return ','.join(str(e).split(',')[3:])

# The returned structure are a list of portals, and a list of SCSIIds for the specified IQN. 
def GetHBAInformation(storage_conf, nolocal=False):
    try:
	retVal = True
	list = []
	scsiIdList = []
	device_config = {}
	HBAFilter = {}

	# Generate a map of the HBAs that the user want to test against.
	if storage_conf['adapters'] != None:
	    for hba in storage_conf['adapters'].split(','):
			HBAFilter[hba] = 1
	
	# Now extract the HBA information from this data.
	try:
	    # the target may not return any IQNs
	    # so prepare for it
            localAdapter = []
            HBADriver = HBA.HBA()
	    devlist= HBADriver.print_devs()
	    TgtList = devlist["Adapter"]
	    for tgt in TgtList:
                if nolocal:
                    if tgt['name'] == 'mpt2sas':
                        localAdapter.append(tgt['host'])
                        continue
	        if len(HBAFilter) != 0:
	    	    if HBAFilter.has_key(tgt['host']):
	    		    list.append(tgt)
	        else:
	    	    list.append(tgt)
	    
	    bdList = devlist["BlockDevice"]
	    for bd in bdList:
	        SCSIid = bd['SCSIid']
	        adapter = ''.join(["host",bd['adapter']])
                if nolocal:
                    if adapter in localAdapter:
                        continue
	        if len(HBAFilter) != 0:
	    	    if HBAFilter.has_key(adapter):
                        scsiIdList.append(SCSIid)
	        else:
	    	    scsiIdList.append(SCSIid)

	    XenCertPrint("The HBA information list being returned is: %s" % list)
        except Exception, e:
	    XenCertPrint("Failed to parse lvmohba probe xml. Exception: %s" % str(e))
	     
    except Exception, e: 
	XenCertPrint("There was an exception in GetHBAInformation: %s." % str(e))
	Print("Exception: %s" % str(e))
	retVal = False
    
    XenCertPrint("GetHBAInformation - returning adapter list: %s and scsi id list: %s." % (list, scsiIdList))  
    return (retVal, list, scsiIdList)

# the following details from the file name, put it into a list and return the list. 
def GetLunInformation(id):
    retVal = True
    listLunInfo = []
    try:
        # take in a host id, then list all files in /dev/disk/by_scsibus of the form *-5* then extract
        deviceInfo = commands.getoutput('lsscsi -i')
        list = []
        for device in deviceInfo.split('\n'):
            info = device.split()
            busid = info[0][1:-1].split(':')
            if busid[0] != id:
                continue
            #fix scsiid lsscsi -i cannot get
            if info[-1].startswith("SATA"):
                info[-1] = commands.getoutput('scsi_id -g %s' % info[-2])
            map = {}
            map['SCSIid'] = info[-1]
            map['id'] = busid[-1]
            map['device'] = info[-2]
            listLunInfo.append(map)
        if len(listLunInfo) == 0:
            retVal = False
    except Exception, e:
        Print("Failed to get lun information for host id: %s, error: %s" % (id, str(e)))
        retVal = False

    return (retVal, listLunInfo)
	    
def PlugAndUnplugPBDs(session, sr_ref, count):
    PrintOnSameLine("      Unplugging and plugging PBDs over %d iterations. Iteration number: " % count)
    try:
	checkPoint = 0;
	for j in range(0, count):
	    PrintOnSameLine(str(j))
	    PrintOnSameLine('..')
	    pbds = session.xenapi.SR.get_PBDs(sr_ref)
	    XenCertPrint("Got the list of pbds for the sr %s as %s" % (sr_ref, pbds))
	    for pbd in pbds:
		XenCertPrint("Looking at PBD: %s" % pbd)
		session.xenapi.PBD.unplug(pbd)
		session.xenapi.PBD.plug(pbd)
	    checkPoint += 1

	PrintOnSameLine('\b\b  ')
	PrintOnSameLine('\n')
    except Exception, e:
	Print("     Exception: %s" % str(e))
	displayOperationStatus(False)
	
    displayOperationStatus(True)
    return checkPoint

def DestroySR(session, sr_ref):	
    try:
	# First get the PBDs
	pbds = session.xenapi.SR.get_PBDs(sr_ref)
	XenCertPrint("Got the list of pbds for the sr %s as %s" % (sr_ref, pbds))
	XenCertPrint(" - Now unplug PBDs for the SR.")
	for pbd in pbds:
	    XenCertPrint("Unplugging PBD: %s" % pbd )                          
	    session.xenapi.PBD.unplug(pbd)	    

	XenCertPrint("Now destroying the SR: %s" % sr_ref)
	session.xenapi.SR.destroy(sr_ref)
	displayOperationStatus(True)
	
    except Exception, e:
	displayOperationStatus(False)
	raise Exception(str(e))
    
def CreateMaxSizeVDIAndVBD(session, sr_ref):
    vdi_ref = None
    vbd_ref = None
    retVal = True
    vdi_size = 0
    
    try:
	try:
	    Print("   Create a VDI on the SR of the maximum available size.")
	    session.xenapi.SR.scan(sr_ref)
	    pSize = session.xenapi.SR.get_physical_size(sr_ref)
	    pUtil = session.xenapi.SR.get_physical_utilisation(sr_ref)
	    #vdi_size = str(actualSRFreeSpace(int(pSize) - int(pUtil)))
	    vdi_size = '1073741824' # wkc hack (1GB)

	    # Populate VDI args
	    args={}
	    args['name_label'] = 'XenCertTestVDI'
	    args['SR'] = sr_ref
	    args['name_description'] = ''
	    args['virtual_size'] = vdi_size
	    args['type'] = 'user'
	    args['sharable'] = False
	    args['read_only'] = False
	    args['other_config'] = {}
	    args['sm_config'] = {}
	    args['xenstore_data'] = {}
	    args['tags'] = []            
	    XenCertPrint("The VDI create parameters are %s" % args)
	    vdi_ref = session.xenapi.VDI.create(args)
	    XenCertPrint("Created new VDI %s" % vdi_ref)
	    displayOperationStatus(True)
	except Exception, e:	    
	    displayOperationStatus(False)
	    raise Exception(str(e))

	Print("   Create a VBD on this VDI and plug it into dom0")
	try:
	    vm_uuid = _get_localhost_uuid()
	    XenCertPrint("Got vm_uuid as %s" % vm_uuid)
	    vm_ref = session.xenapi.VM.get_by_uuid(vm_uuid)
	    XenCertPrint("Got vm_ref as %s" % vm_ref)

	
	    freedevs = session.xenapi.VM.get_allowed_VBD_devices(vm_ref)
	    XenCertPrint("Got free devs as %s" % freedevs)
	    if not len(freedevs):		
		raise Exception("No free devs found for VM: %s!" % vm_ref)
	    XenCertPrint("Allowed devs: %s (using %s)" % (freedevs,freedevs[0]))

	    # Populate VBD args
	    args={}
	    args['VM'] = vm_ref
	    args['VDI'] = vdi_ref
	    args['userdevice'] = freedevs[0]
	    args['bootable'] = False
	    args['mode'] = 'RW'
	    args['type'] = 'Disk'
	    args['unpluggable'] = True 
	    args['empty'] = False
	    args['other_config'] = {}
	    args['qos_algorithm_type'] = ''
	    args['qos_algorithm_params'] = {}
	    XenCertPrint("The VBD create parameters are %s" % args)
	    vbd_ref = session.xenapi.VBD.create(args)
	    XenCertPrint("Created new VBD %s" % vbd_ref)
	    session.xenapi.VBD.plug(vbd_ref)

	    displayOperationStatus(True)
	except Exception, e:
	    displayOperationStatus(False)
	    raise Exception(str(e))
    except Exception, e:
	Print("   Exception creating VDI and VBD, and plugging it into Dom-0 for SR: %s" % sr_ref)
	raise Exception(str(e))
    
    return (retVal, vdi_ref, vbd_ref, vdi_size)

def Attach_VDI(session, vdi_ref, vm_ref):
    vbd_ref = None
    retVal = True
    
    try:
	Print("   Create a VBD on the VDI and plug it into VM requested")
	freedevs = session.xenapi.VM.get_allowed_VBD_devices(vm_ref)
	XenCertPrint("Got free devs as %s" % freedevs)
	if not len(freedevs):		
            XenCertPrint("No free devs found for VM: %s!" % vm_ref)
            return False
	XenCertPrint("Allowed devs: %s (using %s)" % (freedevs,freedevs[0]))

	# Populate VBD args
        args={}
        args['VM'] = vm_ref
        args['VDI'] = vdi_ref
	args['userdevice'] = freedevs[0]
	args['bootable'] = False
	args['mode'] = 'RW'
	args['type'] = 'Disk'
	args['unpluggable'] = True 
	args['empty'] = False
        args['other_config'] = {}
        args['qos_algorithm_type'] = ''
        args['qos_algorithm_params'] = {}
        XenCertPrint("The VBD create parameters are %s" % args)
        vbd_ref = session.xenapi.VBD.create(args)
        XenCertPrint("Created new VBD %s" % vbd_ref)
        session.xenapi.VBD.plug(vbd_ref)

    except Exception, e:
	Print("   Exception Creating VBD and plugging it into VM: %s" % vm_ref)
	return False
    return (retVal, vbd_ref)

def Detach_VDI(session, vdi_ref):
    try:
        vbd_ref = session.xenapi.VDI.get_VBDs(vdi_ref)[0]
        XenCertPrint("vbd_ref is %s"%vbd_ref)
        if vbd_ref != None:
            session.xenapi.VBD.unplug(vbd_ref)
            XenCertPrint("Unplugged VBD %s" % vbd_ref)
            session.xenapi.VBD.destroy(vbd_ref)
            XenCertPrint("Destroyed VBD %s" % vbd_ref)
    except Exception,e:
        raise e
    
def FindTimeToWriteData(devicename, sizeInMiB):
    ddOutFile = 'of=' + devicename
    XenCertPrint("Now copy %dMiB data from /dev/zero to this device and record the time taken to copy it." % sizeInMiB)
    cmd = ['dd', 'if=/dev/zero', ddOutFile, 'bs=4096', 'count=%d' % (sizeInMiB * 256)]
    try:
	(rc, stdout, stderr) = util.doexec(cmd,'')
	list = stderr.split('\n')
	timeTaken = list[2].split(',')[1]
	dataCopyTime = int(float(timeTaken.split()[0]))
	XenCertPrint("The IO test returned rc: %s stdout: %s, stderr: %s" % (rc, stdout, stderr))
	XenCertPrint("Time taken to copy %dMiB to the device %s is %d" % (sizeInMiB, devicename, dataCopyTime))
	return dataCopyTime
    except Exception, e:
	raise Exception(str(e))
		
def PerformSRControlPathTests(session, sr_ref):
    e = None
    try:
	checkPoint = 0
	vdi_ref = None
	vbd_ref = None
	retVal = True	
	
	(retVal, vdi_ref, vbd_ref, vdi_size) = CreateMaxSizeVDIAndVBD(session, sr_ref)
	if not retVal:
	    raise Exception("Failed to create max size VDI and VBD.")
	
	checkPoint += 2
	# Now try to zero out the entire disk 
	Print("   Now attempt to write the maximum number of bytes on this newly plugged device.")
	
	devicename = '/dev/' + session.xenapi.VBD.get_device(vbd_ref)
	XenCertPrint("First finding out the time taken to write 1GB on the device.")
	timeFor512MiBSec = FindTimeToWriteData(devicename, 512)
	timeToWrite = int((float(vdi_size)/(1024*1024*1024)) * (timeFor512MiBSec * 2))
		
	if timeToWrite > timeLimitControlInSec:
	    raise Exception("Writing through this device will take more than %s hours, please use a source upto %s GiB in size." %
			    (timeLimitControlInSec/3600, timeLimitControlInSec/(timeFor512MiBSec * 2)))
	minutes = 0
	hrs = 0
	if timeToWrite > 60:
	    minutes = int(timeToWrite/60)
	    timeToWrite = int(timeToWrite - (minutes * 60))
	    if minutes > 60:
		hrs = int(minutes/60)
		minutes = int(minutes - (hrs * 60))
	
	Print("   START TIME: %s " % (time.asctime(time.localtime())))
	
	if hrs > 0:
	    Print("   APPROXIMATE RUN TIME: %s hours, %s minutes, %s seconds." % (hrs, minutes, timeToWrite))
	elif minutes > 0:
	    Print("   APPROXIMATE RUN TIME: %s minutes, %s seconds." % (minutes, timeToWrite))
	elif timeToWrite > 0:
	    Print("   APPROXIMATE RUN TIME: %s seconds." % (timeToWrite))
	
	ddOutFile = 'of=' + devicename
	bytes = 0
	if not util.zeroOut(devicename, 1, int(vdi_size)):	    
	    raise Exception("   - Could not write through the allocated disk space on test disk, please check the log for the exception details.")
	    displayOperationStatus(False)
	    
	Print("   END TIME: %s " % (time.asctime(time.localtime())))
	displayOperationStatus(True)

	checkPoint += 1
	
    except Exception, e:
	Print("There was an exception performing control path stress tests. Exception: %s" % str(e))
	retVal = False
    
    try:
	# Try cleaning up here
	if vbd_ref != None: 
	    session.xenapi.VBD.unplug(vbd_ref)
	    XenCertPrint("Unplugged VBD %s" % vbd_ref)
	    session.xenapi.VBD.destroy(vbd_ref)
	    XenCertPrint("Destroyed VBD %s" % vbd_ref)

	if vdi_ref != None:
	    session.xenapi.VDI.destroy(vdi_ref)
	    XenCertPrint("Destroyed VDI %s" % vdi_ref)
    except Exception, e:
	Print("- Could not cleanup the objects created during testing, please destroy the vbd %s and vdi %s manually." % (vbd_ref, vdi_ref))
	Print("  Exception: %s" % str(e))
	
    return (checkPoint, retVal)

def get_lun_scsiid_devicename_mapping(targetIQN, portal):
    iscsilib.refresh_luns(targetIQN, portal)
    lunToScsiId={}
    try:
        deviceInfo = commands.getoutput('lsscsi -it')
        for device in deviceInfo.split('\n'):
            info = device.split()
            busId = info[0]
            transport = info[2]
            realPath = info[3]
            #fix scsiid lsscsi -i cannot get
            if info[-1].startswith("SATA"):
                info[-1] = commands.getoutput('scsi_id -g %s' % info[-2])
            scsiId = info[-1]
            if transport[:3] != 'iqn':
                continue
            lunId = busId[1:-1].split(':')[3]
            lunToScsiId[lunId] = (scsiId, realPath)
        return lunToScsiId
    except util.CommandException, inst:
        XenCertPrint("Failed to find any LUNs for IQN: %s and portal: %s" % targetIQN, portal)
        return {}

def parse_config(vendor, product):
    try:
	retVal = True
    	cmd="show config"		
	XenCertPrint("mpath cmd: %s" % cmd)
        (rc,stdout,stderr) = util.doexec(mpath_cli.mpathcmd,cmd)
        XenCertPrint("mpath output: %s" % stdout)
        stdout = stdout.rstrip('}\nmultipaths {\n}\nmultipathd> ') + '\t'
        XenCertPrint("mpath output after stripping: %s" % stdout)
        list = stdout.split("device {")
        skipThis = True
        lastOne = False
        for para in list:
            returnmap = {}
            XenCertPrint("The para is: %s" % para)
            if not skipThis:
	        para = para.lstrip()
                para = para.rstrip('\n\t}\n\t')
                if para.endswith('\n\t}\n}\nblacklist_exceptions {\n}\ndevices {'):
                    para = para.rstrip('\n\t}\n}\nblacklist_exceptions {\n}\ndevices {')
                    lastOne = True
                listParams = para.split('\n\t\t')
                XenCertPrint("ListParams: %s" % listParams)
                for paramPair in listParams:
		    key = ''
		    value = ''
		    params = paramPair.split(' ')
		    firstParam = True
		    for param in params:
			if firstParam:
			    key = param
			    firstParam = False
			    continue
			else:
			    value += param
			    value += ' '
		    value = value.strip()	    
		    returnmap[key] = value
                returnmap['vendor'] = returnmap['vendor'].replace('"', '')
                returnmap['product'] = returnmap['product'].replace('"', '')
                productSearch = '^' + returnmap['product'] + '$'
                vendorSearch = '^' + returnmap['vendor'] + '$'
                regexvendor = re.compile(vendorSearch)
                regexproduct = re.compile(productSearch)
                if ((regexproduct.search(product)) and (regexvendor.search(vendor))):
                    break
                if lastOne:
                    break
            else:
                skipThis = False
    except Exception, e:
        XenCertPrint("Failed to get multipath config for vendor: %s and product: %s. Exception: %s" % (vendor, product, str(e)))
        retVal = False
    # This is not listed in the config, return defaults.
    for key in multiPathDefaultsMap.keys():
	if not returnmap.has_key(key):
	    returnmap[key] = multiPathDefaultsMap[key]
	    
    return (retVal, returnmap )

def parse_xml_config(file):
    configuration = {}
    # predefines if not overriden in config file
    configuration['lunsize'] = '128'
    configuration['growsize'] = '4'

    config_info = xml.dom.minidom.parse(file)
    required = ['adapterid','ssid', 'spid', 'username', 'password', 'target']
    optional = ['port', 'protocol', 'chapuser', 'chappass', 'lunsize', 'growsize']
    for val in required + optional:
       try:
           configuration[val] = str(config_info.getElementsByTagName(val)[0].firstChild.nodeValue)
       except:
           if val in required:
               print "parse exception on REQUIRED ISL option: %s" % val
               raise
           else:
               print "parse exception on OPTIONAL ISL option: %s" % val
    return configuration

#Returns a list of following tuples for the SCSI Id given
#(HBTL, Path dm status, Path status) 
def get_path_status(scsi_id, onlyActive = False):
    listPaths = []
    list = []
    retVal = True
    devname = None
    try:
        lines = mpath_cli.get_topology(scsi_id)
        listPaths = []
        for line in lines:
            m=mpath_cli.regex.search(line)
            if(m):
                listPaths.append(line)

            n=mpath_cli.regex4.search(line)
            if(n):
                devname = line.split()[1].strip()

        XenCertPrint("list_paths returned: %s" % listPaths)

        # Extract hbtl, dm and path status from the multipath topology output
        # e.g. "| |- 0:0:0:0 sda 8:0   active ready running"
        pat = re.compile(r'(\d+:\d+:\d+:\d+.*)$')

        for node in listPaths:
            XenCertPrint("Looking at node: %s" % node)
            match_res = pat.search(node)
            if match_res is None:
                continue

            # Extract path info if pattern matched successfully
            l = match_res.group(1).split()
            hbtl = l[0]
            dm_status = l[3]
            path_status = l[4]
            XenCertPrint("HBTL: %s" % hbtl)
            XenCertPrint("Path status: %s, %s" % (dm_status, path_status))

            if onlyActive:
                if dm_status == 'active':
                    list.append((hbtl, dm_status, path_status))
            else:
                list.append((hbtl, dm_status, path_status))

        XenCertPrint("Returning list: %s" % list)
    except Exception, e:
        XenCertPrint("There was some exception in getting path status for scsi id: %s. Exception: %s" % (scsi_id, str(e)))
        retVal = False

    return (retVal, list, devname)

def _get_localhost_uuid():
    filename = '/etc/xensource-inventory'
    try:
        f = open(filename, 'r')
    except:
        raise xs_errors.XenError('EIO', \
              opterr="Unable to open inventory file [%s]" % filename)
    domid = ''
    for line in filter(util.match_domain_id, f.readlines()):
        domid = line.split("'")[1]
    return domid

def FindDiskDataTestEstimate(device, size):
    estimatedTime = 0
    # Run diskdatatest in a report mode
    XenCertPrint("Run diskdatatest in a report mode with device %s to find the estimated time." % device)
    cmd = [DISKDATATEST, 'report', '1', device]
    DebugCmdArray(cmd)
    XenCertPrint("The command to be fired is: %s" % cmd)
    (rc, stdout, stderr) = util.doexec(cmd)
    if rc == 0:
        lastString = (stdout.split('\n')[-1])
        XenCertPrint("diskdatatest returned : %s" % lastString)
        estimatedTime = int(lastString.split(' ')[-1])
    else:
        XenCertPrint("Diskdatatest return Error : %s" % stderr)
        estimateTime = 0
 
    XenCertPrint("Total estimated time for testing IO with the device %s as %d" % (device, estimatedTime))
    return estimatedTime

def _find_LUN(svid):
    basepath = "/dev/disk/by-csldev/"
    if svid.startswith("NETAPP_"):
        # special attention for NETAPP SVIDs
        svid_parts = svid.split("__")
        globstr = basepath + "NETAPP__LUN__" + "*" + svid_parts[2] + "*" + svid_parts[-1] + "*"
    else:
        globstr = basepath + svid + "*"

    path = util.wait_for_path_multi(globstr, MAX_TIMEOUT)
    if not len(path):
        return []

    #Find CSLDEV paths
    svid_to_use = re.sub("-[0-9]*:[0-9]*:[0-9]*:[0-9]*$","",os.path.basename(path))
    devs = scsiutil._genReverseSCSIidmap(svid_to_use, pathname="csldev")

    #Find scsiID
    for dev in devs:
        try:
            SCSIid = scsiutil.getSCSIid(dev)
        except:
            pass

    #Find root device and return
    if not SCSIid:
        return []
    else:
        device=mpath_dmp.path(SCSIid)
        XenCertPrint("DEBUG: device path : %s"%(device))
        return [device]

def CreateImg(img_path, size=1):
    XenCertPrint("CreateIMG(img_path=%s, size=%s ->Enter)"%(img_path, size))
    try:
        XenCertPrint("about to create image: %s"%img_path)

        f = open(img_path, "w+")
        f.seek(size)
        f.write("junk")
        f.close()
    except Exception,e:
        raise Exception("create image IMG:%s Failed. Error:%s"%(img_path,e))

    XenCertPrint("CreateIMG() -> Exit")

def RemoveImg(img_path):
    XenCertPrint("RemoveIMG(img_path=%s ->Enter)"%img_path)
    try:
        XenCertPrint("about to remove image: %s"%img_path)

        os.remove(img_path)
    except Exception,e:
        raise Exception("remove image IMG:%s Failed. Error:%s"%(img_path,e))

    XenCertPrint("RemoveIMG() -> Exit")


def WriteDataToImg(img_path, startSec, endSec, skipLevel=0, zeroOut=False, full=False):
    XenCertPrint("WriteDataToIMG(img_path=%s, startSec=%s, endSec=%s, skipLevel=%s, full=%s ->Enter)"%(img_path, startSec, endSec, skipLevel, full))
    try:
        XenCertPrint("about to write onto image: %s"%img_path)

        if zeroOut:
            pattern = BUF_ZEROS
        else:
            pattern = BUF_PATTERN

        f = open(img_path, "w+")
        while startSec <= endSec:
            f.seek(startSec * SECTOR_SIZE)
            if full:
                count = 0
                while count < SECTOR_SIZE:
                    f.write(pattern)
                    count += len(pattern)
            else:
                f.write(pattern)
            startSec += 1 + skipLevel
        f.close()
    except Exception,e:
        raise Exception("Writing data into IMG:%s Failed. Error:%s"%(vdi_ref,e))

    XenCertPrint("WriteDataToIMG() -> Exit")

def VerifyDataOnImg(img_path, startSec, endSec, skipLevel=0, zeroed=False, full=False):
    XenCertPrint("VerifyDataOnIMG(img_path=%s, startSec=%s, endSec=%s, skipLevel=%s, full=%s ->Enter)"%(img_path, startSec, endSec, skipLevel, full))
    try:

        XenCertPrint("about to read from image: %s"%img_path)

        if zeroed:
            expect = BUF_ZEROS 
        else:
            expect = BUF_PATTERN

        f = open(img_path, "r+")
        while startSec <= endSec:
            f.seek(startSec * SECTOR_SIZE)
            if full:
                count = 0
                while count < SECTOR_SIZE:
                    actual = f.read(len(expect))
                    if actual != expect:
                        raise Exception("expected:%s <> actual:%s"%(expect, actual))
                    count += len(expect)
            else:
                actual =f.read(len(expect))
                if actual != expect:
                    raise Exception("expected:%s <> actual:%s"%(expect, actual))
            startSec += 1 + skipLevel
        f.close()
    except Exception,e:
        raise Exception("Verification of data in VDI:%s Failed. Error:%s"%(vdi_ref,e))

    XenCertPrint("VerifyDataOnIMG() -> Exit")
