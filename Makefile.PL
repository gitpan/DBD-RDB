# -*- perl -*-

require 5.004;
use strict;

use ExtUtils::MakeMaker;
use DBI::DBD;

my %opts =
    ('NAME' => 'DBD::RDB',
     'AUTHOR' => 'Andreas Stiller (andreas.stiller@eds.com)',
     'VERSION_FROM' => 'RDB.pm',
     'INC' => 'perl_root:[lib.site_perl.VMS_AXP.auto.dbi]',
     'XS' => 'rdb.xs',
     'C' => 'dbdimp.c',
     'OBJECT' => 'rdb.obj dbdimp.obj dbdsql.obj',
     );


ExtUtils::MakeMaker::WriteMakefile(%opts);


package MY;

sub postamble {
DBI::DBD::dbd_postamble().
"
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
