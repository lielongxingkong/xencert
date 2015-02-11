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
import sys
from Logging import Print, PrintR, PrintY, PrintB, PrintG, DebugCmd, DebugCmdArray
from Logging import PrintOnSameLine
from Logging import XenCertPrint
from Logging import displayOperationStatus

class VmHandler:
    def __init__(self, storage_conf):
        XenCertPrint("Reached Vmhandler constructor")
        self.storage_conf = storage_conf
        self.sm_config = {}

    def FunctionalTests(self):
        return (True, 1, 1)
