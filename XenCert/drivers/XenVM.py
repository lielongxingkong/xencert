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
import uuid, shutil

sys.path.insert(0, "../drivers")
sys.path.insert(0, "drivers")

import commands
from Image import Image
from Vif import Vif 
from Logging import Print, PrintR, PrintY, PrintB, PrintG, DebugCmd, DebugCmdArray

DEFAULT_VM_DIRS = ['/mnt',]

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
        vif = Vif()
        vifs.append(vif)
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
    def __init__(self, name, disk_num=1, vif_num=1, vcpus=None, memory=None, first_boot=None, path=None):
        self.name = name
        if path == None:
            for d in DEFAULT_VM_DIRS:
                if os.path.exists(d):
                    self.path = d
                break
        else:
            self.path = path

        # Path Structure
        #     path/root_path/conf_path
        #     input/name/config
        self.root_path = os.path.join(self.path, name)
        self.conf_path = os.path.join(self.root_path, 'config')
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
        if not os.path.exists(self.path):
            raise Exception("Cannot create vm because of non-exists path %s" % self.path)

        os.mkdir(self.root_path)
        self.disks = create_disks(self.disk_num, self.root_path)
        self.vifs = create_vifs(self.vif_num)
        self.store()

    def import_rootdev(self, src_path):
        if len(self.disks) > 0:
            self.disks[0].import_from(src_path)
        else:
            raise Exception("Cannot import disk %s because of no disk on vm" % src_path)
            
    def remove(self):
        shutil.rmtree(self.root_path)

    def store(self):
        with open(self.conf_path, "w") as f:
            for key,val in self._to_dict().items():
                line = key + ' = ' + repr(val) + '\n'
                f.write(line)

    def _to_dict(self):
        config = {}
        config['vcpus'] = self.vcpus
        config['memory'] = self.memory
        config['name'] = self.name
        config['usbdevice'] = self.usbdevice
        config['boot'] = self.boot
        config['splash_time'] = self.splash_time
        config['spice'] = self.spice
        config['builder'] = self.builder
        config['usb'] = self.usb
        config['vnc'] = self.vnc
        config['serial'] = self.serial
        config['xen_platform'] = self.xen_platform_pci
        config['vga'] = self.vga

        disks = []
        for img in self.disks:
            disk = {}
            disk['format'] = 'raw'
            disk['vdev'] = img.name
            disk['backendtype'] = 'qdisk'
            disk['target'] = img.path
            disk = ','.join('%s=%s' % (k,v) for k,v in disk.items())
            disks.append(disk)
        config['disk'] = disks

        vifs = []
        for inf in self.vifs:
            vif = {}
            vif['bridge'] = inf.bridge
            vif['mac'] = inf.addr
            vif['model'] = 'e1000'
            vif['type'] = 'ioemu'
            vif = ','.join('%s=%s' % (k,v) for k,v in vif.items())
            vifs.append(vif)
        config['vif'] = vifs

        return config

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
        while domain_running(self.name):
            time.sleep(2)
            destroy_domain(self.name)

    def print_info(self):
        PrintR('\tVM info:')
        PrintR('\t\tname: %s' % self.name)
        PrintR('\t\tpath: %s' % self.root_path)
        PrintR('\t\tdomain id: %s' % self.domid)
        PrintR('\t\tdisk num: %s' % self.disk_num)
        PrintR('\t\t\tdisks : %s' % ','.join(disk.path for disk in self.disks))
        PrintR('\t\tvif num: %s' % self.vif_num)
        PrintR('\t\t\tvifs : %s' % ','.join(vif.addr for vif in self.vifs))

