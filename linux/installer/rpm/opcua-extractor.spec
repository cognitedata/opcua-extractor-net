%global __os_install_post %{nil}

Name: opcua-extractor
Version: %{extversion}
Release: 1%{?dist}
Obsoletes: %{name} <= %{version}
Summary: Cognite OPC-UA Extractor

License: GPLv2

BuildArch: %{extarch}

%description
Tool to read data from OPC-UA servers into Cognite Data Fusion.
Can be run both as a standalone executable from the command line, or as a systemd service opcua-extractor.

%prep

%build

%install
mkdir -p %{buildroot}%{_bindir}
cp %{name} %{buildroot}%{_bindir}/%{name}
chmod 0755 %{buildroot}%{_bindir}/%{name}

mkdir -p %{buildroot}/etc/cognite/opcua
cp config/opc.ua.net.extractor.Config.xml %{buildroot}/etc/cognite/opcua/
cp config/config.minimal.yml %{buildroot}/etc/cognite/opcua/
cp config/config.example.yml %{buildroot}/etc/cognite/opcua/

mkdir -p %{buildroot}/var/lib/cognite/opcua
mkdir -p %{buildroot}/var/log/cognite/opcua
mkdir -p %{buildroot}/etc/systemd/system
cp %{name}.service %{buildroot}/etc/systemd/system/
cp %{name}@.service %{buildroot}/etc/systemd/system/

%files
%license LICENSE
%{_bindir}/%{name}
/etc/systemd/system/%{name}.service
/etc/systemd/system/%{name}@.service
/etc/cognite/opcua/config.example.yml
/etc/cognite/opcua/config.minimal.yml
/etc/cognite/opcua/opc.ua.net.extractor.Config.xml
/var/log/cognite/opcua/
/var/lib/cognite/opcua/

%post
ln -sfn /var/log/cognite/opcua/ /var/lib/cognite/opcua/logs
ln -sfn /etc/cognite/opcua/ /var/lib/cognite/opcua/config

%preun
if [ $1 -eq 0 ]
then
    rm -r /var/lib/cognite/opcua/*
fi

%changelog
 * Thu Aug 19 2021 Einar Omang <einar.omang@cognite.com> - 2.4.0
 - Initial release of rpm package

