# -*- rpm-spec -*-

Summary: The Inspur Xen Storage Certification Kit
Name:    inspur-xencert
Version: 0.1
Release: 0
Group:   System/Hypervisor
License: LGPL
URL:  https://github.com/lielongxingkong/xencert
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-build

%description
XenCert is the automated testkit for certifying storage hardware with Inspur InCloud Sphere.

%prep
%setup -q

%build
cd $RPM_BUILD_DIR/%{name}-%{version}/diskdatatest;make

%install
python setup.py install --prefix=%{_prefix} --root=%{buildroot}
install -m 0755 -D scripts/inspur-xencert $RPM_BUILD_ROOT/usr/bin/
install -m 0755 -D diskdatatest/diskdatatest $RPM_BUILD_ROOT/opt/inspur/XenCert/diskdatatest
install -m 0755 scripts/* $RPM_BUILD_ROOT/opt/inspur/XenCert/

%clean
rm -rf $RPM_BUILD_ROOT

%files
#TODO python version compatible
/usr/lib/python2.6/site-packages/XenCert/*
/usr/lib/python2.6/site-packages/inspur_xencert-0.1-py2.6.egg-info/*
/opt/inspur/XenCert/*
/usr/bin/inspur-xencert

%changelog
