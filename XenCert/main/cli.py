#!/usr/bin/python -u
#
# Copyright (C) Citrix Systems Inc.
# Copyright (C) Inspur Inc.
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

"""Manual Inspur InCloud Sphere Certification script"""

import os
import sys
import XenCertCommon
from Logging import Print, PrintR, PrintG, PrintY, PrintB
from Logging import PrintToLog
from Logging import InitLogging
from Logging import UnInitLogging
from Logging import GetLogFileName
from datetime import datetime
import time, commands

def main():
    """Main Routine"""

    pass_all = True
    g_storage_conf = {}
    InitLogging()
    PrintToLog("***********************************************************************\n")
    PrintToLog("The Inspur XenCert command executed is: \n\n")
    for arg in sys.argv:
	PrintToLog(arg)
	PrintToLog(' ')
    PrintToLog('\n\n')
    (options, args) = XenCertCommon.parse_args("%prog 2.5. \nInspur InCloud Sphere, Storage Certification.\n")
   
    if args:
        Print("Unknown arguments found: %s" % args)
        sys.exit(1)

    if options.help:
	XenCertCommon.DisplayUsage()
	return 0
	
    if not XenCertCommon.valid_arguments(options, g_storage_conf):
        return 1

    XenCertCommon.store_configuration(g_storage_conf, options)
    
    # Now is the time to instantiate the right handler based on the
    # requested storage type and hand it over to the handler to
    # perform the certification process
    handler = XenCertCommon.GetStorageHandler(g_storage_conf)
    PrintB("********************** Welcome to Inspur InCloud Sphere XenCert *****************")
    Print("Test start time: %s" % time.asctime(time.localtime()))
    start = datetime.now()
    Print("***********************************************************************")  
    testAll = False
    
    if not options.functional and not options.multipath and not options.data:
	    testAll = True
    
    if options.storage_type == 'vm':
        options.functional = options.multipath = options.data = testAll = False
        Print("Performing multipath configuration verification.")
        (retValVM, checkPointsVM, totalCheckPointsVM) = handler.FunctionalTests()
        if checkPointsVM != totalCheckPointsVM:
            pass_all = False
        Print("***********************************************************************")
        timeOfCompletionVM = time.asctime(time.localtime())

    if options.multipath or testAll:
	Print("Performing multipath configuration verification.")
    	(retValMP, checkPointsMP, totalCheckPointsMP) = handler.MPConfigVerificationTests()
        if checkPointsMP != totalCheckPointsMP:
            pass_all = False
	Print("***********************************************************************")
	timeOfCompletionMP = time.asctime(time.localtime())

    if options.functional or testAll: 
    	Print("Performing functional tests.")
    	(retValFunctional, checkPointsFunctional, totalCheckPointsFunctional) = handler.FunctionalTests()
        if checkPointsFunctional != checkPointsFunctional:
            pass_all = False
    	Print("***********************************************************************")
	timeOfCompletionFunctional = time.asctime(time.localtime())

    if options.data or testAll: 
    	Print("Performing data IO tests.")
    	(retValData, checkPointsData, totalCheckPointsData) = handler.DataPerformanceTests()
        if checkPointsData != checkPointsData:
            pass_all = False
        Print("***********************************************************************")
    timeOfCompletionData = time.asctime(time.localtime())
    
    PrintB("Result of Inspur InCloud Sphere XenCert certification suite.")

    # Now display all the results
    if options.storage_type == 'vm':
	Print("***********************************************************************")
	if retValVM: 
            PrintG("%-50s: PASS, Pass percentage: %d, Completed: %s" %
		  ('Virtual machine test results', int((checkPointsVM * 100)/totalCheckPointsVM), timeOfCompletionVM))
	else:
            PrintR("%-50s: FAIL, Pass percentage: %d, Completed: %s" %
		  ('Virtual machine test results', int((checkPointsVM * 100)/totalCheckPointsVM), timeOfCompletionVM))
 
    if options.multipath or testAll:
	Print("***********************************************************************")
	if retValMP: 
            PrintG("%-50s: PASS, Pass percentage: %d, Completed: %s" %
		  ('Multipath configuration verification results', int((checkPointsMP * 100)/totalCheckPointsMP), timeOfCompletionMP))
	else:
            PrintR("%-50s: FAIL, Pass percentage: %d, Completed: %s" %
		  ('Multipath configuration verification results', int((checkPointsMP * 100)/totalCheckPointsMP), timeOfCompletionMP))
    	
    if options.functional or testAll:     	
    	Print("***********************************************************************")
	if retValFunctional: 
            PrintG("%-50s: PASS, Pass percentage: %d, Completed: %s" %
		  ('Functional test results', int((checkPointsFunctional * 100)/totalCheckPointsFunctional), timeOfCompletionFunctional))
	else:
            PrintR("%-50s: FAIL, Pass percentage: %d, Completed: %s" %
		  ('Functional test results', int((checkPointsFunctional * 100)/totalCheckPointsFunctional), timeOfCompletionFunctional))

    if options.data or testAll:         
        Print("***********************************************************************")
        if retValData: 
            PrintG("%-50s: PASS, Pass percentage: %d, Completed: %s" %
          ('Data test results', int((checkPointsData * 100)/totalCheckPointsData), timeOfCompletionData))
    	else:
            PrintR("%-50s: FAIL, Pass percentage: %d, Completed: %s" %
          ('Data test results', int((checkPointsData * 100)/totalCheckPointsData), timeOfCompletionData))
        
    Print("***********************************************************************")
    PrintB("End of Inspur InCloud Sphere XenCert certification suite.")
    Print("Please find the report for this test run at: %s" % GetLogFileName())
    Print("***********************************************************************")
    end = datetime.now()
    Print("Test end time: %s" % time.asctime(time.localtime()))
    timed = end - start
    hr = timed.seconds/3600
    min = timed.seconds%3600/60
    sec = timed.seconds%60
    if hr > 0:
	Print("Execution time: %d hours, %d minutes, %d seconds." % (hr, min, sec))
    elif min > 0:
	Print("Execution time: %d minutes, %d seconds." % (min, sec))
    elif sec > 0:
        Print("Execution time: %d seconds." % sec)	    
    
    Print("***********************************************************************")  
    UnInitLogging()

    if pass_all:
        sys.exit(0)
    else:
        sys.exit(1)
    
if __name__ == "__main__":
    main()
