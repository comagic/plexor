%global version         2.2
%global pgmajorversion  12
%define pgbaseinstdir   /usr
%global oname           plexor-pg%{pgmajorversion}

Summary:   Plexor - remote function call PL language
Name:      %{oname}
Version:   %{version}
Release:   1%{?dist}
Group:     Applications/Databases
License:   BSD
Source0:   %{oname}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{oname}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: postgresql-devel >= %{pgmajorversion}
Requires:      postgresql >= %{pgmajorversion}

%description
Plexor - remote function call PL language

%prep
%setup -q -n %{oname}-%{version}

%build
USE_PGXS=1 make %{?_smp_mflags}

%install
rm -rf %{buildroot}
USE_PGXS=1 make %{?_smp_mflags} install DESTDIR=%{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{pgbaseinstdir}/%{_lib}/pgsql/plexor.so
%{pgbaseinstdir}/share/pgsql/extension/plexor--%{version}.sql
%{pgbaseinstdir}/share/pgsql/extension/plexor.control
