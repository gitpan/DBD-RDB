#
#   Copyright (c) 2000 Andreas Stiller
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.
#
require 5.003;
package DBD::RDB;

use DBI();
use DynaLoader ();
use Exporter ();

@ISA = qw(DynaLoader Exporter);

$VERSION = 1.13;
$ABSTRACT = "RDB driver for DBI";

use strict;
use vars qw($VERSION $ABSTRACT $err $errstr $state $drh $sqlstate);

bootstrap DBD::RDB $VERSION;

$err = 0;		# holds error code   for DBI::err
$errstr = "";	        # holds error string for DBI::errstr
$sqlstate = "";       # holds SQL state for    DBI::state

  sub driver {
    return $drh if $drh;	# already created - return same one
    my($class, $attr) = @_;

    $class .= "::dr";

    # not a 'my' since we use it above to prevent multiple drivers
    $drh = DBI::_new_drh($class, {
      'Name'    => 'File',
      'Version' => $VERSION,
      'Err'     => \$DBD::File::err,
      'Errstr'  => \$DBD::File::errstr,
      'State'   => \$DBD::File::state,
      'Attribution' => 'DBD::RDB $Version using dynamic SQL by Andreas Stiller',
    });

    return $drh;
  }

package DBD::RDB::dr; # ====== DRIVER ======
use strict;

sub connect {
    my($drh, $dbname, $user, $auth, $attr)= @_;

    # Some database specific verifications, default settings
    # and the like following here. This should only include
    # syntax checks or similar stuff where it's legal to
    # 'die' in case of errors.

    # create a 'blank' dbh (call superclass constructor)
    my $dbh = DBI::_new_dbh($drh, {
      'Name' => $dbname
    });

    DBD::RDB::db::_login( $dbh, $dbname, $user, $auth ) or return undef;
    $dbh;
}


package DBD::RDB::db; # ====== DATABASE ======
use strict;

sub prepare {
    my ( $dbh, $statement, $attribs ) = @_;

    # create a 'blank' sth
    my $sth = DBI::_new_sth($dbh, {} );
    DBD::RDB::st::_prepare( $sth, $statement, $attribs ) or return undef;
    $sth;
}

sub do {
    my ( $dbh, $statement, $attr, @bind_values ) = @_;
    
    if ( $attr || @bind_values ) {
	my $sth = $dbh->prepare( $statement, $attr ) or return undef;
        $sth->execute( @bind_values );
    } else {
        DBD::RDB::db::_do( $dbh, $statement );
    }
}


sub tables {
    my ( $dbh ) = @_;

    my @tables;
    my ( $table, $sth, $catalog, $schema );

    local $dbh->{RaiseError} = 0;
    local $dbh->{PrintError} = 0;

    my $started = $dbh->do( "set transaction read only" );
    $sth = $dbh->prepare( q/
	select s2.rdb$catalog_schema_name,
	       s1.rdb$catalog_schema_name,
	       s.rdb$stored_name
	  from rdb$catalog.rdb$schema.rdb$catalog_schema s1,
	       rdb$catalog.rdb$schema.rdb$catalog_schema s2,
	       rdb$catalog.rdb$schema.rdb$synonyms s,
	       rdb$catalog.rdb$schema.rdb$relations r
	 where r.rdb$system_flag = 0
	   and r.rdb$relation_name = s.rdb$stored_name
	   and s.rdb$schema_id = s1.rdb$catalog_schema_id
	   and s1.rdb$parent_id = s2.rdb$catalog_schema_id / );


    if ( $sth ) {
	$sth->bind_columns( \$catalog, \$schema, \$table );
	$sth->execute;
	while ( $sth->fetch ) {
	    push @tables, "$catalog.$schema.$table";
	}
        $sth->finish;
    } else {
        $sth = $dbh->prepare( q/
                  select rdb$relation_name
                    from rdb$relations
                   where rdb$system_flag = 0 / );
	if ( $sth ) {
	    $sth->bind_columns( \$table );
	    $sth->execute;
	    while ( $sth->fetch ) {
	        push @tables, $table;
	    }
            $sth->finish;
	} else {
	    die $sth->errstr if not $sth;
	}
    }
    $dbh->commit if $started;
    return @tables;
}    

1;


__END__

=head1 NAME

DBD::RDB - Oracle RDB database driver for the DBI module

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:RDB:ATTACH FILENAME <rootfile>" );

  # The $user and $passwd parameters of the standard connect are unused.
  # They should be included instead in the ATTACH if needed for a remote
  # connection


=head1 DESCRIPTION

DBD::Oracle is a Perl module which works with the DBI module to provide
access to Oracle RDB databases (tested with RDB 7, probably also 6).

=head1 CONNECTING TO RDB 

The connect use the syntax of the CONNECT TO statement. The example above
connects to a database with the rootfile <rootfile> and use the standard RDB
alias. If inside SQL a connect works with

SQL> CONNECT TO 'connect-string';

then the next command should also work:

  $dbh = DBI->connect("dbi:RDB:connect-string");

An example with a second alias inside the same connection is

  $dbh = DBI->connect("dbi:RDB:ATTACH FILENAME <rootfile-1>, 
                               ATTACH ALIAS A FILENAME <rootfile-2>" );

Multiple connects (i.e. multiple $dbh's) are supported using the 
SET CONNECT inside the DBD.

=head1 Caveats

Segmented strings are not implemented
cursor parameters like in DBD::Oracle does not work

=head1 AUTHOR

DBD::RDB by Andreas Stiller. DBI by Tim Bunce.

=head1 COPYRIGHT

The DBD::RDB module is Copyright (c) 2000 Andreas Stiller. Germany.
The DBD::RDB module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself with the exception that it
cannot be placed on a CD-ROM or similar media for commercial distribution
without the prior approval of the author.

=cut
