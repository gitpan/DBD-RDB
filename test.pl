use DBI;
$dbh = DBI->connect('dbi:RDB:', undef, undef,
                       { RaiseError => 1, PrintError => 1, AutoCommit => 0 } );
$dbh->do('create database filename test');
$dbh->disconnect;
$dbh = DBI->connect('dbi:RDB: ATTACH FILENAME TEST', undef, undef,
                       { RaiseError => 1, PrintError => 1, AutoCommit => 0 } );
$dbh->do( q/create table dummy (  a char(10),
				  b varchar(20),
	                          c int,
				  d date vms,
				  e int(2) )/ );
$dbh->commit;
$dbh->do( 'set transaction read write');
$st = $dbh->prepare( q/insert into dummy (a,b,c,d,e) values(?,?,?,?,?)/ );
$st->execute( 'a1', 'b111111', 1, '1-JAN-2000', '1.11' );
$st->execute( 'a2', 'b222', 2, '2-FEB-2000', '2.22' );
$st->execute( 'a3', 'b3', 3, '3-MAR-2000', '3.3' );
$dbh->commit;
#
# time/date conversion done with LIB$xxxDATExxx routines and can be influenced
# by LIB$DTxxx logicals
#
$st = $dbh->prepare( q/select d,e from dummy where c = ?/ );
$st->bind_columns( \$d_var, \$e_var );
$st->execute( 3 );
while ( $st->fetch ) {
    print "d: $d_var e: $e_var\n";
}
