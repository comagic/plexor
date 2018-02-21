%global version         2.1.0
%global pgmajorversion  9.6
%global pgmimorversion  1
%define pgbaseinstdir   /usr
%global oname           plexor

Summary:   Plexor - remote function call PL language
Name:      %{oname}
Version:   %{version}
Release:   2%{?dist}
Group:     Applications/Databases
License:   BSD
Source0:   %{oname}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: postgresql-devel >= %{pgmajorversion}.%{pgmimorversion} flex >= 2.5.37
Requires:      postgresql >= %{pgmajorversion}.%{pgmimorversion}

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
%{pgbaseinstdir}/share/pgsql/extension/plexor--2.1.sql
%{pgbaseinstdir}/share/pgsql/extension/plexor.control
