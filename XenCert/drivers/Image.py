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

import util, commands
import os, uuid

DEFAULT_IMG_DIRS=['/mnt', ] 

class Image(object):
    def __init__(self, size, store_dir=None, name=None):
        self.size = size  #size in MB
        if name == None:
            self.name = str(uuid.uuid4())
        else:
            self.name = name

        if store_dir == None:
            for path in DEFAULT_IMG_DIRS:
                if os.path.exits(path):
                    self.store_dir = path
                    break
            raise Exception("Cannot choose a path to save image %s" % self.name)
        else:
            if os.path.exists(store_dir):
                self.store_dir = store_dir
            else:
                raise Exception("Cannot set path %s to save image %s" % (store_dir, self.name))

        self.path = os.path.join(self.store_dir, self.name)

        self.sparse_size = None  #size in MB
        self.read_only = False

    def create(self):
        commands.getoutput('qemu-img create %s %sM' % (self.path, self.size))
        if self.read_only:
            commands.getoutput('chmod %s 444' % self.path)
  
    def get_sparse_size(self, force_refresh=True):
        if self.sparse_size != None and not force_refresh:
            return self.sparse_size

        output = commands.getoutput('du -m s' % self.path)
        sparse_size = output.split()[0]
        try:
            self.sparse_size = int(sparse_size)
            return self.sparse_size
        except ValueError, e:
            raise Exception("Cannot get sparse size of image " % str(e))
        

