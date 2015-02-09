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

import sys
import util
import os
import re
import commands
import time

logfile = None
logfilename = None

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
