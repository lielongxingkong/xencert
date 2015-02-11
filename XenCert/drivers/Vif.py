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
#

import os
import random
import commands

class Vif(object):
    def __init__(self, bridge=None, mac_addr=None):
        if bridge == None:
            self.bridge = "manageNetwork"
        else:
            bridges = commands.getoutput('ovs-vsctl list-br').split('\n')
            if bridge not in bridges:
                raise Exception('bridge %s does not exists' % bridge)
            else:
                self.bridge = bridge

        if mac_addr == None:
            self.addr = self._random_mac()
        else:
            self.addr = mac_addr

    def _random_mac(self):
        mac = [0x00, 0x16, 0x3e,
            random.randint(0x00, 0x7f),
            random.randint(0x00, 0xff),
            random.randint(0x00, 0xff)]
        return ':'.join(map(lambda x: '%02x' % x, mac))

