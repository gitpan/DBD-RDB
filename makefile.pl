# -*- perl -*-

require 5.004;
use strict;

use ExtUtils::MakeMaker;
use DBI::DBD;
use Config;

my $obj_ext = $Config{'obj_ext'} || '.obj';

my %opts =
    ('NAME' => 'DBD::RDB',
     'AUTHOR' => 'Andreas Stiller (andreas.stiller@eds.com)',
     'PMLIBDIRS' => [qw(DBD)],
     'VERSION_FROM' => 'rdb.pm',
     'INC' => 'perl_root:[lib.site_perl.VMS_AXP.auto.dbi]',
     'XS' => 'rdb.xs',
     'C' => 'dbdimp.c',
     'OBJECT' => "rdb$obj_ext dbdimp$obj_ext dbdsql$obj_ext",
     );


ExtUtils::MakeMaker::WriteMakefile(%opts);


package MY;

sub postamble {
DBI::DBD::dbd_postamble().
"
.FIRST
      define lnk\$library sys\$library:sql\$user.olb

.SUFFIXES .sqlmod

.sqlmod.obj :
      mc sql\$mod \$(mms\$source) /c_proto/connect/warn=nodeprecate

dbdimp.obj : dbdsql.obj

";
}

sub libscan {
    my($self, $path) = @_;
    ($path =~ /\~$/) ? undef : $path;
}
