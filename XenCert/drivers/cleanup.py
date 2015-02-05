#!/usr/bin/python
# Copyright (C) 2006-2007 XenSource Ltd.
# Copyright (C) 2008-2009 Citrix Ltd.
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
# Script to coalesce and garbage collect VHD-based SR's in the background
#

import os
import sys
import time
import signal
import subprocess
import getopt
import datetime
import exceptions
import traceback
import base64
import zlib

import util
import lvutil
import vhdutil
import lvhdutil
import lvmcache
import journaler
import fjournaler
import lock
from refcounter import RefCounter
from ipc import IPCFlag
from lvmanager import LVActivator

# Disable automatic leaf-coalescing. Online leaf-coalesce is currently not 
# possible due to lvhd_stop_using_() not working correctly. However, we leave 
# this option available through the explicit LEAFCLSC_FORCE flag in the VDI 
# record for use by the offline tool (which makes the operation safe by pausing 
# the VM first)
AUTO_ONLINE_LEAF_COALESCE_ENABLED = True

FLAG_TYPE_ABORT = "abort"     # flag to request aborting of GC/coalesce

# process "lock", used simply as an indicator that a process already exists 
# that is doing GC/coalesce on this SR (such a process holds the lock, and we 
# check for the fact by trying the lock). 
LOCK_TYPE_RUNNING = "running" 
lockRunning = None


class AbortException(util.SMException):
    pass

################################################################################
#
#  Util
#
class Util:
    RET_RC     = 1
    RET_STDOUT = 2
    RET_STDERR = 4

    UUID_LEN = 36

    PREFIX = {"G": 1024 * 1024 * 1024, "M": 1024 * 1024, "K": 1024}

    def log(text):
        util.SMlog(text, ident="SMGC")
    log = staticmethod(log)

    def logException(tag):
        info = sys.exc_info()
        if info[0] == exceptions.SystemExit:
            # this should not be happening when catching "Exception", but it is
            sys.exit(0)
        tb = reduce(lambda a, b: "%s%s" % (a, b), traceback.format_tb(info[2]))
        Util.log("*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*")
        Util.log("         ***********************")
        Util.log("         *  E X C E P T I O N  *")
        Util.log("         ***********************")
        Util.log("%s: EXCEPTION %s, %s" % (tag, info[0], info[1]))
        Util.log(tb)
        Util.log("*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*~*")
    logException = staticmethod(logException)

    def doexec(args, expectedRC, inputtext=None, ret=None, log=True):
        "Execute a subprocess, then return its return code, stdout, stderr"
        proc = subprocess.Popen(args,
                                stdin=subprocess.PIPE,\
                                stdout=subprocess.PIPE,\
                                stderr=subprocess.PIPE,\
                                shell=True,\
                                close_fds=True)
        (stdout, stderr) = proc.communicate(inputtext)
        stdout = str(stdout)
        stderr = str(stderr)
        rc = proc.returncode
        if log:
            Util.log("`%s`: %s" % (args, rc))
        if type(expectedRC) != type([]):
            expectedRC = [expectedRC]
        if not rc in expectedRC:
            reason = stderr.strip()
            if stdout.strip():
                reason = "%s (stdout: %s)" % (reason, stdout.strip())
            Util.log("Failed: %s" % reason)
            raise util.CommandException(rc, args, reason)

        if ret == Util.RET_RC:
            return rc
        if ret == Util.RET_STDERR:
            return stderr
        return stdout
    doexec = staticmethod(doexec)

    def runAbortable(func, ret, ns, abortTest, pollInterval, timeOut):
        """execute func in a separate thread and kill it if abortTest signals
        so"""
        abortSignaled = abortTest() # check now before we clear resultFlag
        resultFlag = IPCFlag(ns)
        resultFlag.clearAll()
        pid = os.fork()
        if pid:
            startTime = time.time()
            while True:
                if resultFlag.test("success"):
                    Util.log("  Child process completed successfully")
                    resultFlag.clear("success")
                    return
                if resultFlag.test("failure"):
                    resultFlag.clear("failure")
                    raise util.SMException("Child process exited with error")
                if abortTest() or abortSignaled:
                    os.killpg(pid, signal.SIGKILL)
                    raise AbortException("Aborting due to signal")
                if timeOut and time.time() - startTime > timeOut:
                    os.killpg(pid, signal.SIGKILL)
                    resultFlag.clearAll()
                    raise util.SMException("Timed out")
                time.sleep(pollInterval)
        else:
            os.setpgrp()
            try:
                if func() == ret:
                    resultFlag.set("success")
                else:
                    resultFlag.set("failure")
            except:
                resultFlag.set("failure")
            os._exit(0)
    runAbortable = staticmethod(runAbortable)

    def num2str(number):
        for prefix in ("G", "M", "K"):
            if number >= Util.PREFIX[prefix]:
                return "%.3f%s" % (float(number) / Util.PREFIX[prefix], prefix)
        return "%s" % number
    num2str = staticmethod(num2str)

    def numBits(val):
        count = 0
        while val:
            count += val & 1
            val = val >> 1
        return count
    numBits = staticmethod(numBits)

    def countBits(bitmap1, bitmap2):
        """return bit count in the bitmap produced by ORing the two bitmaps"""
        len1 = len(bitmap1)
        len2 = len(bitmap2)
        lenLong = len1
        lenShort = len2
        bitmapLong = bitmap1
        if len2 > len1:
            lenLong = len2
            lenShort = len1
            bitmapLong = bitmap2

        count = 0
        for i in range(lenShort):
            val = ord(bitmap1[i]) | ord(bitmap2[i])
            count += Util.numBits(val)

        for i in range(i + 1, lenLong):
            val = ord(bitmapLong[i])
            count += Util.numBits(val)
        return count
    countBits = staticmethod(countBits)

    def getThisScript():
        thisScript = util.get_real_path(__file__)
        if thisScript.endswith(".pyc"):
            thisScript = thisScript[:-1]
        return thisScript
    getThisScript = staticmethod(getThisScript)


################################################################################
#
#  Helpers
#
def daemonize():
    pid = os.fork()
    if pid:
        os.waitpid(pid, 0)
        Util.log("New PID [%d]" % pid)
        return False
    os.chdir("/")
    os.setsid()
    pid = os.fork()
    if pid:
        Util.log("Will finish as PID [%d]" % pid)
        os._exit(0)
    for fd in [0, 1, 2]:
        try:
            os.close(fd)
        except OSError:
            pass
    # we need to fill those special fd numbers or pread won't work
    sys.stdin = open("/dev/null", 'r')
    sys.stderr = open("/dev/null", 'w')
    sys.stdout = open("/dev/null", 'w')
    return True

def normalizeType(type):
    if type in LVHDSR.SUBTYPES:
        type = SR.TYPE_LVHD
    if type in ["lvm", "lvmoiscsi", "lvmohba"]:
        # temporary while LVHD is symlinked as LVM
        type = SR.TYPE_LVHD
    if type in ["ext", "nfs"]:
        type = SR.TYPE_FILE
    if not type in SR.TYPES:
        raise util.SMException("Unsupported SR type: %s" % type)
    return type

def _gcLoop(sr, dryRun):
    failedCandidates = []
    while True:
        if not sr.xapi.isPluggedHere():
            Util.log("SR no longer attached, exiting")
            break
        sr.scanLocked()
        if not sr.hasWork():
            Util.log("No work, exiting")
            break

        if not lockRunning.acquireNoblock():
            Util.log("Another instance already running, exiting")
            break
        try:
            if not sr.gcEnabled():
                break
            sr.cleanupCoalesceJournals()
            sr.scanLocked()
            sr.updateBlockInfo()

            if len(sr.findGarbage()) > 0:
                sr.garbageCollect(dryRun)
                sr.xapi.srUpdate()
                continue

            candidate = sr.findCoalesceable()
            if candidate:
                util.fistpoint.activate("LVHDRT_finding_a_suitable_pair",sr.uuid)
                sr.coalesce(candidate, dryRun)
                sr.xapi.srUpdate()
                continue

            candidate = sr.findLeafCoalesceable()
            if candidate:
                sr.coalesceLeaf(candidate, dryRun)
                sr.xapi.srUpdate()
                continue

            Util.log("No work left")
            sr.cleanup()
        finally:
            lockRunning.release()

def _gc(session, srUuid, dryRun):
    init(srUuid)
    sr = SR.getInstance(srUuid, session)
    if not sr.gcEnabled(False):
        return

    sr.cleanupCache()
    try:
        _gcLoop(sr, dryRun)
    finally:
        sr.cleanup()
        sr.logFilter.logState()
        del sr.xapi

def _abort(srUuid):
    """If successful, we return holding lockRunning; otherwise exception
    raised."""
    Util.log("=== SR %s: abort ===" % (srUuid))
    init(srUuid)
    if not lockRunning.acquireNoblock():
        gotLock = False
        Util.log("Aborting currently-running instance (SR %s)" % srUuid)
        abortFlag = IPCFlag(srUuid)
        abortFlag.set(FLAG_TYPE_ABORT)
        for i in range(SR.LOCK_RETRY_ATTEMPTS):
            gotLock = lockRunning.acquireNoblock()
            if gotLock:
                break
            time.sleep(SR.LOCK_RETRY_INTERVAL)
        abortFlag.clear(FLAG_TYPE_ABORT)
        if not gotLock:
            raise util.SMException("SR %s: error aborting existing process" % \
                    srUuid)

def init(srUuid):
    global lockRunning
    if not lockRunning:
        lockRunning = lock.Lock(LOCK_TYPE_RUNNING, srUuid) 

def usage():
    output = """Garbage collect and/or coalesce VHDs in a VHD-based SR

Parameters:
    -u --uuid UUID   SR UUID
 and one of:
    -g --gc          garbage collect, coalesce, and repeat while there is work
    -G --gc_force    garbage collect once, aborting any current operations
    -c --cache-clean <max_age> clean up IntelliCache cache files older than
                     max_age hours
    -a --abort       abort any currently running operation (GC or coalesce)
    -q --query       query the current state (GC'ing, coalescing or not running)
    -x --disable     disable GC/coalesce (will be in effect until you exit)
    -t --debug       see Debug below

Options:
    -b --background  run in background (return immediately) (valid for -g only)
    -f --force       continue in the presence of VHDs with errors (when doing
                     GC, this might cause removal of any such VHDs) (only valid
                     for -G) (DANGEROUS)

Debug:
    The --debug parameter enables manipulation of LVHD VDIs for debugging
    purposes.  ** NEVER USE IT ON A LIVE VM **
    The following parameters are required:
    -t --debug <cmd> <cmd> is one of "activate", "deactivate", "inflate",
                     "deflate".
    -v --vdi_uuid    VDI UUID
    """
   #-d --dry-run     don't actually perform any SR-modifying operations
    print output
    Util.log("(Invalid usage)")
    sys.exit(1)


##############################################################################
#
#  API
#
def abort(srUuid):
    """Abort GC/coalesce if we are currently GC'ing or coalescing a VDI pair.
    """
    _abort(srUuid)
    Util.log("abort: releasing the process lock")
    lockRunning.release()

def gc(session, srUuid, inBackground, dryRun = False):
    """Garbage collect all deleted VDIs in SR "srUuid". Fork & return 
    immediately if inBackground=True. 
    
    The following algorithm is used:
    1. If we are already GC'ing in this SR, return
    2. If we are already coalescing a VDI pair:
        a. Scan the SR and determine if the VDI pair is GC'able
        b. If the pair is not GC'able, return
        c. If the pair is GC'able, abort coalesce
    3. Scan the SR
    4. If there is nothing to collect, nor to coalesce, return
    5. If there is something to collect, GC all, then goto 3
    6. If there is something to coalesce, coalesce one pair, then goto 3
    """
    Util.log("=== SR %s: gc ===" % srUuid)
    if inBackground:
        if daemonize():
            # we are now running in the background. Catch & log any errors 
            # because there is no other way to propagate them back at this 
            # point
            
            try:
                _gc(None, srUuid, dryRun)
            except AbortException:
                Util.log("Aborted")
            except Exception:
                Util.logException("gc")
                Util.log("* * * * * SR %s: ERROR\n" % srUuid)
            os._exit(0)
    else:
        _gc(session, srUuid, dryRun)

def gc_force(session, srUuid, force = False, dryRun = False, lockSR = False):
    """Garbage collect all deleted VDIs in SR "srUuid". The caller must ensure
    the SR lock is held.
    The following algorithm is used:
    1. If we are already GC'ing or coalescing a VDI pair, abort GC/coalesce
    2. Scan the SR
    3. GC
    4. return
    """
    Util.log("=== SR %s: gc_force ===" % srUuid)
    init(srUuid)
    sr = SR.getInstance(srUuid, session, lockSR, True)
    if not lockRunning.acquireNoblock():
        _abort(srUuid)
    else:
        Util.log("Nothing was running, clear to proceed")

    if force:
        Util.log("FORCED: will continue even if there are VHD errors")
    sr.scanLocked(force)
    sr.cleanupCoalesceJournals()

    try:
        sr.cleanupCache()
        sr.garbageCollect(dryRun)
    finally:
        sr.cleanup()
        sr.logFilter.logState()
        lockRunning.release()

def get_state(srUuid):
    """Return whether GC/coalesce is currently running or not. The information
    is not guaranteed for any length of time if the call is not protected by
    locking.
    """
    init(srUuid)
    if lockRunning.acquireNoblock():
        lockRunning.release()
        return False
    return True

def should_preempt(session, srUuid):
    sr = SR.getInstance(srUuid, session)
    entries = sr.journaler.getAll(VDI.JRN_COALESCE)
    if len(entries) == 0:
        return False
    elif len(entries) > 1:
        raise util.SMException("More than one coalesce entry: " + entries)
    sr.scan()
    coalescedUuid = entries.popitem()[0]
    garbage = sr.findGarbage()
    for vdi in garbage:
        if vdi.uuid == coalescedUuid:
            return True
    return False

def get_coalesceable_leaves(session, srUuid, vdiUuids):
    coalesceable = []
    sr = SR.getInstance(srUuid, session)
    sr.scanLocked()
    for uuid in vdiUuids:
        vdi = sr.getVDI(uuid)
        if not vdi:
            raise util.SMException("VDI %s not found" % uuid)
        if vdi.isLeafCoalesceable():
            coalesceable.append(uuid)
    return coalesceable

def cache_cleanup(session, srUuid, maxAge):
    sr = SR.getInstance(srUuid, session)
    return sr.cleanupCache(maxAge)

def debug(sr_uuid, cmd, vdi_uuid):
    Util.log("Debug command: %s" % cmd)
    sr = SR.getInstance(sr_uuid, None)
    if not isinstance(sr, LVHDSR):
        print "Error: not an LVHD SR"
        return
    sr.scanLocked()
    vdi = sr.getVDI(vdi_uuid)
    if not vdi:
        print "Error: VDI %s not found"
        return
    print "Running %s on SR %s" % (cmd, sr)
    print "VDI before: %s" % vdi
    if cmd == "activate":
        vdi._activate()
        print "VDI file: %s" % vdi.path
    if cmd == "deactivate":
        ns = lvhdutil.NS_PREFIX_LVM + sr.uuid
        sr.lvmCache.deactivate(ns, vdi.uuid, vdi.fileName, False)
    if cmd == "inflate":
        vdi.inflateFully()
        sr.cleanup()
    if cmd == "deflate":
        vdi.deflate()
        sr.cleanup()
    sr.scanLocked()
    print "VDI after:  %s" % vdi

##############################################################################
#
#  CLI
#
def main():
    action     = ""
    uuid       = ""
    background = False
    force      = False
    dryRun     = False
    debug_cmd  = ""
    vdi_uuid   = ""
    shortArgs  = "gGc:aqxu:bfdt:v:"
    longArgs   = ["gc", "gc_force", "clean_cache", "abort", "query", "disable",
            "uuid=", "background", "force", "dry-run", "debug=", "vdi_uuid="]

    try:
        opts, args = getopt.getopt(sys.argv[1:], shortArgs, longArgs)
    except getopt.GetoptError:
        usage()
    for o, a in opts:
        if o in ("-g", "--gc"):
            action = "gc"
        if o in ("-G", "--gc_force"):
            action = "gc_force"
        if o in ("-c", "--clean_cache"):
            action = "clean_cache"
            maxAge = int(a)
        if o in ("-a", "--abort"):
            action = "abort"
        if o in ("-q", "--query"):
            action = "query"
        if o in ("-x", "--disable"):
            action = "disable"
        if o in ("-u", "--uuid"):
            uuid = a
        if o in ("-b", "--background"):
            background = True
        if o in ("-f", "--force"):
            force = True
        if o in ("-d", "--dry-run"):
            Util.log("Dry run mode")
            dryRun = True
        if o in ("-t", "--debug"):
            action = "debug"
            debug_cmd = a
        if o in ("-v", "--vdi_uuid"):
            vdi_uuid = a

    if not action or not uuid:
        usage()
    if action == "debug" and not (debug_cmd and vdi_uuid) or \
            action != "debug" and (debug_cmd or vdi_uuid):
        usage()

    if action != "query" and action != "debug":
        print "All output goes to log"

    if action == "gc":
        gc(None, uuid, background, dryRun)
    elif action == "gc_force":
        gc_force(None, uuid, force, dryRun, True)
    elif action == "clean_cache":
        cache_cleanup(None, uuid, maxAge)
    elif action == "abort":
        abort(uuid)
    elif action == "query":
        print "Currently running: %s" % get_state(uuid)
    elif action == "disable":
        print "Disabling GC/coalesce for %s" % uuid
        _abort(uuid)
        raw_input("Press enter to re-enable...")
        print "GC/coalesce re-enabled"
        lockRunning.release()
    elif action == "debug":
        debug(uuid, debug_cmd, vdi_uuid)


if __name__ == '__main__':
    main()
