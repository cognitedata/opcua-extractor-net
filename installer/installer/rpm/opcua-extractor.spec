Name: opcua-extractor
Version: %{ext-version}
Release: 1%{?dist}
Summary: Cognite OPC-UA Extractor

License: GPLv2

BuildArch: %{ext-arch}

%description
Tool to read data from OPC-UA servers into Cognite Data Fusion.
Can be run both as a standalone executable from the command line, or as a systemd service opcua-extractor.

%prep
%setup -q

%build

%install
mkdir -p %{buildroot}/%{_bindir}
install -m 0755 %{name} %{buildroot}/%{_bindir}/%{name}

mkdir -p %{buildroot}/etc/cognite/opcua
cp config/opc.ua.net.extractor.Config.xml %{buildroot}/etc/cognite/opcua/
cp config/config.minimal.yml %{buildroot}/etc/cognite/opcua/
cp config/config.example.yml %{buildroot}/etc/cognite/opcua/

mkdir -p %{buildroot}/%{_sharedstatedir}/cognite/opcua
mkdir -p %{buildroot}/%{_localstatedir}/log/cognite/opcua
mkdir -p %{buildroot}/etc/systemd/system
cp %{name}.service %{buildroot}/etc/systemd/system/
cp %{name}@.service %{buildroot}/etc/systemd/system/

%files
%license LICENSE
%{_bindir}/%{name}
etc/systemd/system/%{name}.service
etc/systemd/system/%{name}@.service
etc/cognite/opcua/config.example.yml
etc/cognite/opcua/config.minimal.yml
etc/cognite/opcua/opc.ua.net.extractor.Config.xml

%post
ln -sfn /%{_localstatedir}/log/cognite/opcua/ /%{_sharedstatedir}/cognite/opcua/logs
ln -sfn /etc/cognite/opcua/ /%{_sharedstatedir}/cognite/opcua/config

%preun
rm -r /var/lib/cognite/opcua/*

%changelog
 * Thu Aug 19 2021 Einar Omang <einar.omang@cognite.com> - 2.4.0
 - Initial release of rpm package

