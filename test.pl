use DBI;
use strict;

my $print_error = 0;
print "1..45\n";

#
#  load driver
#
my $ok = require DBD::RDB;
printf("%sok 1\n", ($ok ? "" : "not "));

#
# create test database;
#
eval {
    my $dbh = DBI->connect( 'dbi:RDB:', undef, undef,
			{ RaiseError => 0, 
			  PrintError => $print_error, 
			  AutoCommit => 0 } );
    $dbh->do('create database filename test');
    $dbh->disconnect;
};
printf("%sok 2\n", ($@ ? "not" : ""));

#
# try to connect to a non existing database, expect error
#
my $dbh = DBI->connect( 'dbi:RDB: ATTACH FILENAME XXXX.RDB', undef, undef,
                        { RaiseError => 0,
			  PrintError => $print_error, 
			  AutoCommit => 0 } );
printf("%sok 3\n", (($DBI::errstr =~ /RDB\-E\-BAD_DB_FORMAT/) ? "" : "not "));

#
# connect to fresh test database
#
$dbh = DBI->connect( 'dbi:RDB: ATTACH FILENAME TEST.RDB', undef, undef,
                     { RaiseError => 0,
	               PrintError => $print_error, 
		       AutoCommit => 0,
		       ChopBlanks => 1 } );
printf("%sok 4\n", ($dbh ? "" : "not "));

#
#  create test table
#
$ok = $dbh->do( "set transaction read write" );
printf("%sok 5\n", (( $ok && !$DBI::errstr) ? "" : "not "));

$ok = $dbh->do( q/create table dummy (  
		    col_char char(15),
		    col_varchar varchar(30),
		    col_int int,
		    col_float float,
		    col_date date vms,
		    col_decimal decimal(5),
		    col_quad bigint,
		    col_intp2 int(2) ) / );
printf("%sok 6\n", (( $ok && !$DBI::errstr) ? "" : "not "));

$ok = $dbh->commit;
printf("%sok 7\n", (( $ok && !$DBI::errstr) ? "" : "not "));


#
# get back default dateformat
#
my $df = $dbh->{rdb_dateformat};
$ok = ( $df eq "|!Y4!MN0!D0|!H04!M0!S0!C2|" );
printf("%sok 8\n", (( $ok && !$DBI::errstr) ? "" : "not "));

#
# check ChopBlanks is on
#
my $cb = $dbh->{ChopBlanks};
$ok = ( $cb == 1 );
printf("%sok 9\n", (( $ok && !$DBI::errstr) ? "" : "not "));

#
#  start read write again to insert something
#
$ok = $dbh->do( 'set transaction read write');
printf("%sok 10\n", (( $ok && !$DBI::errstr) ? "" : "not "));

#
#  prepare insert
#
my $st_text = q/
insert into dummy (
   col_char,
   col_varchar,
   col_int,
   col_float,
   col_date,
   col_decimal,
   col_quad,
   col_intp2 )
values ( ?, ?, ?, ?, ?, ?, ?, ? )/;
my $st = $dbh->prepare( $st_text );
printf("%sok 11\n", (( $ok && !$DBI::errstr) ? "" : "not "));

#
# check some statement handle attributes
#
$ok = ( $st->{Statement} eq $st_text );
printf("%sok 12\n", (( $ok ) ? "" : "not "));

$ok = ( $st->{NUM_OF_PARAMS} == 8 );
printf("%sok 13\n", (( $ok ) ? "" : "not "));

$ok = ( $st->{NUM_OF_FIELDS} == 0 );
printf("%sok 14\n", (( $ok ) ? "" : "not "));

#
#  now two inserts
#
my $col_char_t1		= 'Abcdef';
my $col_varchar_t1	= 'skjdsdalskhd';
my $col_int_t1		= 12345;
my $col_float_t1        = 7654.12E12;
my $col_date_t1		= '19630709';
my $col_decimal_t1	= 54321;
my $col_quad_t1		= '123456789012';
my $col_intp2_t1	= '321.12';

$ok = $st->execute( 
    $col_char_t1,
    $col_varchar_t1,
    $col_int_t1,
    $col_float_t1,
    $col_date_t1,
    $col_decimal_t1,
    $col_quad_t1,
    $col_intp2_t1 );
printf("%sok 15\n", (( $ok && !$DBI::errstr) ? "" : "not "));

my $col_char_t2		= 'BCDEFGHJ';
my $col_varchar_t2	= 'jhfjsdhfshfkljhd';
my $col_int_t2		= 4242442;
my $col_float_t2        = -123.5678E100;
my $col_date_t2		= '20000229 120030';
my $col_decimal_t2	= -54321; 
my $col_quad_t2		= '-98765432101';
my $col_intp2_t2	= '-44.44';

$ok = $st->execute( 
    $col_char_t2,
    $col_varchar_t2,
    $col_int_t2,
    $col_float_t2,
    $col_date_t2,
    $col_decimal_t2,
    $col_quad_t2,
    $col_intp2_t2 );
printf("%sok 16\n", (( $ok && !$DBI::errstr) ? "" : "not "));


$ok = $dbh->commit;
printf("%sok 17\n", (( $ok && !$DBI::errstr) ? "" : "not "));

#
#  prepare select to check the inserted values;
#
$ok = $dbh->do( 'set transaction read only');
printf("%sok 18\n", (( $ok && !$DBI::errstr) ? "" : "not "));

$st = $dbh->prepare( q/
select col_char, col_varchar, col_int, col_float as col_floating,
       col_date, col_decimal, col_quad, col_intp2
  from dummy
 order by col_char /);
printf("%sok 19\n", (( $st && !$DBI::errstr) ? "" : "not "));

#
#  check the NAME attribute (name of the select columns)
#
$ok = ( $st->{NUM_OF_FIELDS} == 8 );
printf("%sok 20\n", (( $ok ) ? "" : "not "));

my $names = $st->{NAME};
$ok = ( 8 == @$names &&
        $$names[0] eq "COL_CHAR" &&
	$$names[1] eq "COL_VARCHAR" &&
	$$names[2] eq "COL_INT" &&
	$$names[3] eq "COL_FLOATING" &&
	$$names[4] eq "COL_DATE" &&
	$$names[5] eq "COL_DECIMAL" &&
	$$names[6] eq "COL_QUAD" &&
	$$names[7] eq "COL_INTP2" );
printf("%sok 21\n", (( $ok ) ? "" : "not "));

$names = $st->{NAME_uc};
$ok = ( 8 == @$names &&
        $$names[0] eq "COL_CHAR" &&
	$$names[1] eq "COL_VARCHAR" &&
	$$names[2] eq "COL_INT" &&
	$$names[3] eq "COL_FLOATING" &&
	$$names[4] eq "COL_DATE" &&
	$$names[5] eq "COL_DECIMAL" &&
	$$names[6] eq "COL_QUAD" &&
	$$names[7] eq "COL_INTP2" );
printf("%sok 22\n", (( $ok ) ? "" : "not "));

$names = $st->{NAME_lc};
$ok = ( 8 == @$names &&
        $$names[0] eq "col_char" &&
	$$names[1] eq "col_varchar" &&
	$$names[2] eq "col_int" &&
	$$names[3] eq "col_floating" &&
	$$names[4] eq "col_date" &&
	$$names[5] eq "col_decimal" &&
	$$names[6] eq "col_quad" &&
	$$names[7] eq "col_intp2" );
printf("%sok 23\n", (( $ok ) ? "" : "not "));

#
#  check the column name attribute
#
my $types = $st->{TYPE};
#foreach my $type ( @$types ) {
#    print "type: $type\n";
#}
$ok = ( 8 == @$types &&
        $$types[0] == 453 && # CHAR
	$$types[1] == 449 && # VARCHAR
	$$types[2] == 497 && # INTEGER
	$$types[3] == 481 && # FLOAT
        $$types[4] == 503 && # DATE
	$$types[5] == 497 && # DECIMAL(5) -> INTEGER
	$$types[6] == 505 && # QUADWORD
	$$types[7] == 497 ); # INTEGER
printf("%sok 24\n", (( $ok ) ? "" : "not "));

#
#  check the precsion attribute
#
my $precisions = $st->{PRECISION};
#foreach my $prec ( @$precisions ) {
#    print "prec: $prec\n";
#}
$ok = ( 8 == @$precisions &&
        $$precisions[0] == 15 &&
        $$precisions[1] == 30 &&
        $$precisions[2] == 9 &&
        $$precisions[3] == 17 && # double precision
        $$precisions[4] == 17 && # default formt is "dd.mm.yyyy hh.mm.ss"
				 # changes with date format choosen during
				 # connect
        $$precisions[5] == 9 &&  # DECIMAL(5) -> INTEGER, the (5) is NA
        $$precisions[6] == 18 && # QUADWORD
	$$precisions[7] == 9 );  # INTEGER
printf("%sok 25\n", (( $ok ) ? "" : "not "));

#
#  check the scale attribute
#
my $scales = $st->{SCALE};
#foreach my $scale ( @$scales ) {
#    print "scale: $scale\n";
#}
$ok = ( 8 == @$scales &&
        !defined $$scales[0] && # CHAR
	!defined $$scales[1] && # VARCHAR
	$$scales[2] == 0 &&     # INTEGER
	$$scales[3] == 0 &&     # FLOAT
        !defined $$scales[4] && # DATE
	$$scales[5] == 0 &&     # DECIMAL(5) -> INTEGER
	$$scales[6] == 0 &&     # QUADWORD
	$$scales[7] == 2 );     # INTEGER(2)
printf("%sok 26\n", (( $ok ) ? "" : "not "));

#
#  check the nullable attribute, to get it right one has to check 
#  NOT NULL constraints, this is not implemented
#
my $nullables = $st->{NULLABLE};
#foreach my $nullable ( @$nullables ) {
#    print "nullable: $nullable\n";
#}
$ok = ( 8 == @$nullables &&
	$$nullables[0] == 2 &&    # constraints are not checked, -> unknown
	$$nullables[1] == 2 &&
	$$nullables[2] == 2 &&
	$$nullables[3] == 2 &&
	$$nullables[4] == 2 &&
	$$nullables[5] == 2 &&
	$$nullables[6] == 2 &&
	$$nullables[7] == 2 );
printf("%sok 27\n", (( $ok ) ? "" : "not "));

#
#  check the cursor name attribute
#
my $st_name = $st->{CursorName};
#print "statement name: $st_name\n";
$ok = ( $st_name =~ /^CUR_(\d+)$/ );
printf("%sok 28\n", (( $ok ) ? "" : "not "));

#
#  bind columns
#
my ( $col_char, $col_varchar, $col_int, $col_float, $col_date, $col_decimal,
     $col_quad, $col_intp2 );
$ok = $st->bind_columns ( \$col_char,
			  \$col_varchar,
			  \$col_int,
			  \$col_float,
			  \$col_date,
			  \$col_decimal,
			  \$col_quad,
			  \$col_intp2 );
printf("%sok 29\n", (( $st && !$DBI::errstr) ? "" : "not "));

$ok = $st->execute;
printf("%sok 30\n", (( $st && !$DBI::errstr) ? "" : "not "));

#
#  first fetch and compare
#
$ok = $st->fetch;
printf("%sok 31\n", (( $st && !$DBI::errstr) ? "" : "not "));

#print "col_char     $col_char\n";
#print "col_varchar  $col_varchar\n";
#print "col_int      $col_int    \n";
#print "col_float    $col_float  \n";
#print "col_date     $col_date   \n";
#print "col_decimal  $col_decimal\n";
#print "col_quad     $col_quad   \n";
#print "col_intp2    $col_intp2  \n";

$ok = ( $col_char eq $col_char_t1 &&
        $col_varchar eq $col_varchar_t1 &&
	$col_float == $col_float_t1 &&
	$col_int == $col_int_t1 &&
	$col_date =~ /^$col_date_t1/ &&
	$col_decimal == $col_decimal_t1 &&
	$col_quad eq $col_quad_t1 &&
	$col_intp2 eq $col_intp2_t1 );
printf("%sok 32\n", (( $ok ) ? "" : "not "));

#
#  second fetch and compare
#
$ok = $st->fetch;
printf("%sok 33\n", (( $st && !$DBI::errstr) ? "" : "not "));

#print "col_char     $col_char\n";
#print "col_varchar  $col_varchar\n";
#print "col_int      $col_int    \n";
#print "col_float    $col_float  \n";
#print "col_date     $col_date   \n";
#print "col_decimal  $col_decimal\n";
#print "col_quad     $col_quad   \n";
#print "col_intp2    $col_intp2  \n";

$ok = ( $col_char eq $col_char_t2 &&
        $col_varchar eq $col_varchar_t2 &&
	$col_float == $col_float_t2 &&
	$col_int == $col_int_t2 &&
	$col_date =~ /^$col_date_t2/ &&
	$col_decimal == $col_decimal_t2 &&
	$col_quad eq $col_quad_t2 &&
	$col_intp2 eq $col_intp2_t2 );
printf("%sok 34\n", (( $ok ) ? "" : "not "));

$ok = $st->fetch;
printf("%sok 35\n", (( !$ok && !$DBI::errstr ) ? "" : "not "));

$ok = $dbh->commit;
printf("%sok 36\n", (( $ok && !$DBI::errstr ) ? "" : "not "));

$ok = $dbh->disconnect;
printf("%sok 37\n", (( $ok && !$DBI::errstr ) ? "" : "not "));

#
#  connect again with VMS standard date format
#
$dbh = DBI->connect( 'dbi:RDB: ATTACH FILENAME TEST.RDB', undef, undef,
                     { RaiseError => 1,
	               PrintError => $print_error, 
		       AutoCommit => 0,
		       ChopBlanks => 1,
		       rdb_dateformat => '|!DB-!MAAU-!Y4|!H04:!M0:!S0.!C2|' } );
printf("%sok 38\n", ($dbh ? "" : "not "));

#
#  check date conversion
#
eval {
    $dbh->do( "set transaction read only" );
    my $st = $dbh->prepare( "select col_date from dummy where " .
			    "col_char = ?" );
    $st->bind_columns( \$col_date );
    $st->execute( $col_char_t1 );
    $st->fetch;
    $st->finish;
    $dbh->commit;
};
#print "err: $@, result: $col_date\n";
$ok = ( !$@ && $col_date eq " 9-JUL-1963 00:00:00.00" );
printf("%sok 39\n", ($ok ? "" : "not "));

#
#  check NULL handling
#
eval {
    $dbh->do( "set transaction read write" );
    my $sts = "update dummy set col_varchar = ?, col_int = ?, col_float = ?," .
           "col_date = ?, col_decimal = ?, col_quad = ?, col_intp2 = ? " .
	   "where col_char = ?";
    my $st = $dbh->prepare( $sts );
    $st->execute( undef, undef, undef, undef, undef, undef, undef,
	          $col_char_t1 );
    $st->finish;
    $dbh->commit;

    $dbh->do( "set transaction read only" );
    $st = $dbh->prepare( q/select col_varchar, col_int, col_float,
                                  col_date, col_decimal, col_quad, col_intp2
	                   from dummy where col_char = ?/ );

    $st->bind_columns ( \$col_varchar, \$col_int, \$col_float,\$col_date,
		        \$col_decimal, \$col_quad, \$col_intp2 );
    $st->execute( $col_char_t1 );
    $st->fetch;
    $st->finish;
    $dbh->commit;
};
$ok = ( !$@ && 
        !defined $col_varchar &&
        !defined $col_int &&
        !defined $col_float &&
        !defined $col_date &&
        !defined $col_decimal &&
        !defined $col_quad &&
        !defined $col_intp2 );
printf("%sok 40\n", ($ok ? "" : "not "));

#
#  checking $dbh->tables method
#
my @tables = $dbh->tables;
#foreach my $table ( @tables ) {
#    print "table $table\n";
#}
$ok = ( !$DBI::errstr && @tables == 1 && $tables[0] eq "DUMMY" );
printf("%sok 41\n", ($ok ? "" : "not "));

#
#  insert 50% NULL and 50% NOT NULL columns to test for a special bug
#  occured for multiple inserts
#
my $count;
eval {
    $dbh->do( "set transaction read write" );
    $dbh->{rdb_dateformat} = q/|!Y4!MN0!D0|!H04!M0!S0!C2|/;
    $st = $dbh->prepare( q/insert into dummy ( col_char, col_varchar, 
				col_int, col_float, col_date, col_decimal, 
				col_quad, col_intp2 )
		           values( ?, ?, ?, ?, ?, ?, ?, ? ) / );
    for ( 1 .. 100 ) { 
	if ( $_ % 2 ) {
	    $st->execute( 'test_null', $col_varchar_t2, $col_int_t2,
                          $col_float_t2, $col_date_t2, $col_decimal_t2,
			  $col_quad_t2, $col_intp2_t2 );
	} else {
	    $st->execute( 'test_null', undef, undef,
			  undef, undef, undef,
			  undef, undef );
	}
    }
    $st->finish;
    $dbh->commit;
    $dbh->do( "set transaction read only" );
    $count = $dbh->selectrow_array( "select count(*) from dummy " .
	  			    "where col_varchar is not null " .
				    "and col_char = 'test_null'" );
    $dbh->commit;
};
$ok = ( !$@ && $count == 50 );
printf("%sok 42\n", ($ok ? "" : "not "));

#
#  check HOLD cursor which endures a commit
#
eval {
    $dbh->do( "set transaction read write" );
    my $st1 = $dbh->prepare( "select col_date from dummy where " .
			     "col_char = 'test_null'", { rdb_hold => 1 } );
    my $st2 = $dbh->prepare( "update dummy set col_varchar = 'gulp' " .
			     "where current of $st1->{CursorName}" );
    $st1->execute;
    while ( $st1->fetch ) {
	$st2->execute;
	$st2->finish;
	$dbh->commit;
	$dbh->do( "set transaction read write" );
    }
    $dbh->commit;
    $dbh->do( "set transaction read only" );
    $count = $dbh->selectrow_array( "select count(*) from dummy " .
	  			    "where col_varchar = 'gulp' " .
				    "and col_char = 'test_null'" );
    $dbh->commit;
};		
$ok = ( !$@ && $count == 100 );
printf("%sok 43\n", ($ok ? "" : "not "));


#
#  checking the complex $dbh->do using bind_values which is not using
#  the driver internal exec immediate (like all dos before)
#
eval {
    $dbh->do( "set transaction read write" );
    $dbh->do( "delete from dummy where col_varchar = ?", undef, 'gulp' );
    $dbh->commit;

    $dbh->do( "set transaction read only" );
    $count =  $dbh->selectrow_array( 
	"select count(*) from dummy where col_varchar = ?", undef, 'gulp' );
    
    $dbh->commit;
};
$ok = ( !$@ && $count == 0 );
printf("%sok 44\n", ($ok ? "" : "not "));

#
#  check for regression of the following bug:
#  ChopBlanks off and reading a numeric column with precision returns
#  the number as string but with trailing blanks. String is OK, but trailing
#  blanks not !
#
$dbh->{ChopBlanks} = 0;
eval {
    $dbh->do( "set transaction read only" );
    ($col_char,$col_intp2) =  $dbh->selectrow_array( 
	"select col_char,col_intp2 from dummy where col_char = '$col_char_t2'",
                                            undef );
		# see line 138 and below    
    $dbh->commit;
};
#print "|$col_char|$col_intp2|\n";
$ok = ( !$@ && $col_intp2 eq $col_intp2_t2 && 
        length($col_char) == 15 && $col_char =~ /^$col_char_t2 \s+$/x );
printf("%sok 45\n", ($ok ? "" : "not "));

$dbh->disconnect;





