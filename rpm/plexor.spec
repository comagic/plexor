%global version         0.2.2
%global pgmajorversion  9.4
%global pgmimorversion  5
%define pgbaseinstdir   /usr
%global oname           plexor

Summary:   Plexor - remote function call PL language
Name:      %{oname}
Version:   %{version}
Release:   1%{?dist}
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
%{pgbaseinstdir}/share/pgsql/extension/plexor--0.1.sql
%{pgbaseinstdir}/share/pgsql/extension/plexor.control
