#!/usr/bin/python
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

import os, sys, re, time
import uuid
import ConfigParser

sys.path.insert(0, "../drivers")
sys.path.insert(0, "drivers")

from Logging import Print, PrintR, PrintY, PrintB, PrintG, DebugCmd, DebugCmdArray

def get_boot(first=None):
    order = ''
    default = ['d', 'n', 'c']
    
    if first in ('cd', 'c'):
        order += 'c'
        default.remove('c')
    elif first in ('disk', 'd'):
        order += 'd'
        default.remove('d')
    elif first in ('net', 'n'):
        order += 'n'
        default.remove('n')
    elif first == None:
        pass
    else:
        raise Exception("Unknown boot type %s" % first)

    order += ''.join(default)
    return order

def write_to_config(XenVM):
    pass

def read_from_config(config_path):
    pass

def create_disks(disk_num, path):
    disks = []
    for i in range(disk_num):
        img = Image(1000, path, 'xvd' + chr(ord('a') + i))
        img.create()
        disks.append(img) 
    return disks

def create_vifs(vif_num):
    vifs = []
    for i in range(vif_num):
        pass
    return vifs 

def domain_running(name):
    if domain_id(name) != None:
        return True
    else:
        return False

def domain_id(name):
    output = commands.getoutput('xl domid %s' % name)
    try:
        return int(output)
    except ValueError, e:
        return None

def create_domain(path):
    commands.getoutput('xl cr %s' % path)
    
def destroy_domain(name):
    commands.getoutput('xl des %s' % name)

class XenVM():
    """Xen Virtual Machine Class"""
    def __init__(self, name, conf_dir, disk_num, vif_num, vcpus=None, memory=None, first_boot=None):
        self.name = name
        self.conf_path = os.path.join(conf_dir, name)
        self.uuid = str(uuid.uuid4())

        if vcpus == None:
            self.vcpus = 1
        else:
            self.vcpus = vcpus

        if memory == None:
            self.memory = 1024
        else:
            self.memory = memory 
           
        self.boot = get_boot(first_boot)
        
        self.disk_num = disk_num
        self.vif_num = vif_num

        self.domid = None

        self.usbdevice = "tablet"
        self.splash_time = 0
        self.spice = 0
        self.builder = "hvm"
        self.usb = 1
        self.vnc = 1
        self.serial = "pty"
        self.xen_platform_pci = 1
        self.vga = "stdvga"

    def create(self):
        os.mkdir(self.root_path)
        self.disks = create_disks(self.disk_num, self.root_path)
        self.vifs = create_vifs(self.vif_num)
        self.store()
            
    def remove(self):
        pass

    def load(self):
        cf = ConfigParser.ConfigParser()
        cf.read(self.conf_path)

    def store(self):
        dic = self._to_dict()
        cf = ConfigParser.ConfigParser()
        cf.write(open(self.conf_path, "w"))

    def _to_dict(self):
        return {}

    def start(self):
        create_domain(self.conf_path)
        retries = 5
        while True:
            time.sleep(2)
            self.domid = domain_id(self.name)
            retries -= 1
            if self.domid != None or retries == 0:
                break

    def stop(self):
        destroy_domain(self.name)
        while not domain_running(self.name):
            time.sleep(2)
            destroy_domain(self.name)

