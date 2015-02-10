#!/usr/bin/env python
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
# FileSystem: Based on local-file storage datastore

import util, scsiutil

import os
import uuid, commands
import cleanup
from Logging import Print, PrintR, PrintY, PrintB, PrintG, DebugCmd, DebugCmdArray

MOUNT_BASE = '/media'

class FileSystem(object):
    """Local file storage repository"""
    def __init__(self, device, name=None, path=None):
        self.device = device
        if name == None:
            self.name = str(uuid.uuid4())
        else:
            self.name = name

        if not self._isvalidpathstring(self.device):
            raise Exception('Config device invalid %s' % dev)

        if path == None:
            self.path = os.path.join(MOUNT_BASE, self.name)
        else:
            self.path = path
        if not self._isvalidpathstring(self.path):
            raise Exception('Invalid path %s' % self.path)

        self.attached = self._checkmount()

    def delete(self):
        self.detach()

    def attach(self):
        if not self._checkmount():
            try:
                # make a mountpoint:
                if not os.path.isdir(self.path):
                    os.makedirs(self.path)
            except util.CommandException, inst:
                raise Exception('Error make mountpoint.')
            
            try:
                util.pread(["fsck", "-a", self.device])
            except util.CommandException, inst:
                if inst.code == 1:
                    util.SMlog("FSCK detected and corrected FS errors. Not fatal.")
                else:
                    raise Exception('FSCK failed on %s.' % self.device)

            try:
                util.pread(["mount", self.device, self.path])
                self.attached = True
            except util.CommandException, inst:
                raise Exception('Failed to mount FS.')

    def detach(self):
        if not self._checkmount():
            return
        cleanup.abort(self.uuid)
        try:
            # Change directory to avoid unmount conflicts
            os.chdir(MOUNT_BASE)
            
            # unmount the device
            util.pread(["umount", self.path])

            # remove the mountpoint
            os.rmdir(self.path)
            self.path = None

            self.attached = False
        except util.CommandException, inst:
            raise Exception('Failed to unmount FS.')

    def create(self, size):
        if self._checkmount():
            raise Exception('Mounted FS.')
        self.mkfs()

    def mkfs(self):
        try:
            util.pread2(["mkfs.ext3", "-F", self.device])
        except util.CommandException, inst:
            raise Exception('mkfs failed')

    def _checkmount(self):
        return self.path and os.path.ismount(self.path)
    
    def _isvalidpathstring(self, path):
        if not path.startswith("/"):
            return False
        l = self._splitstring(path)
        for char in l:
            if char.isalpha():
                continue
            elif char.isdigit():
                continue
            elif char in ['/','-','_','.',':']:
                continue
            else:
                return False
        return True


class EXT4(FileSystem):
    def mkfs(self):
        try:
            util.pread2(["mkfs.ext3", "-F", self.device])
        except util.CommandException, inst:
            raise Exception('mkfs.ext4 failed')


class OCFS2(FileSystem):
    def mkfs(self):
        try:
            util.pread2(["mkfs.ext3", "-F", self.device])
        except util.CommandException, inst:
            raise Exception('mkfs.ocfs failed')


class XFS(FileSystem):
    def mkfs(self):
        try:
            util.pread2(["mkfs.ext3", "-F", self.device])
        except util.CommandException, inst:
            raise Exception('mkfs.xfs failed')

