#include <descrip>
#include <builtins>
#include <limits.h>
#include <lib$routines>
#include <starlet>
#include <ssdef>
#include <sql_literals>
#include <ints>
#include <libdtdef>
#include <descrip>

#include "rdb.h"
#include "dbdsql.h"
#include "sql.h"


DBISTATE_DECLARE;

/* XXX DBI should provide a better version of this */
#define IS_DBI_HANDLE(h) \
    (SvROK(h) && SvTYPE(SvRV(h)) == SVt_PVHV && \
	SvRMAGICAL(SvRV(h)) && (SvMAGIC(SvRV(h)))->mg_type == 'P')

void dbd_init( dbistate_t *dbistate ) {
    DBIS = dbistate;
}


static int do_error( SV *h, long rdb_status, long vms_status )
{
    char err_buf[1024];
    int err_txtlen;
    struct dsc$descriptor_s err_dsc;
    extern int SQL_GET_ERROR_TEXT();

    D_imp_xxh(h);
    SV *errstr = DBIc_ERRSTR(imp_xxh);

    if ( rdb_status ) {
	SQL_GET_ERROR_TEXT( err_buf, sizeof(err_buf)-1, &err_txtlen );
	err_buf[err_txtlen] = 0;
	sv_setpv( errstr, err_buf );
	sv_setiv( DBIc_ERR(imp_xxh), (IV)rdb_status );
        DBIh_EVENT2( h, ERROR_event, DBIc_ERR(imp_xxh), errstr );
    } else {
	err_dsc.dsc$a_pointer = err_buf,
	err_dsc.dsc$w_length = sizeof(err_buf) - 1;
	err_dsc.dsc$b_dtype = DSC$K_DTYPE_T;
	err_dsc.dsc$b_class = DSC$K_CLASS_S;
	SYS$GETMSG ( vms_status, &err_txtlen, &err_dsc, 0, 0 );
	sv_setpv( errstr, err_buf );
	sv_setiv( DBIc_ERR(imp_xxh), (IV)vms_status );
        DBIh_EVENT2( h, ERROR_event, DBIc_ERR(imp_xxh), errstr );
    }
    return 0;
}

void set_date_format( SV* dbh, imp_dbh_t *imp_dbh, 
	              char *format_str, int format_len )
{
    int status;
    struct dsc$descriptor_s format_dsc;
    int64 now;
    char now_str[256];
    struct dsc$descriptor_s now_dsc;

    if (dbis->debug >= 2) {
	fprintf(DBILOGFP, "set date_format %s\n", format_str );
    }

    strncpy( imp_dbh->date_format, format_str, 
	     sizeof(imp_dbh->date_format) );

    format_dsc.dsc$b_dtype = DSC$K_DTYPE_T;
    format_dsc.dsc$b_class = DSC$K_CLASS_S;
    format_dsc.dsc$a_pointer = format_str;
    format_dsc.dsc$w_length = format_len;
    imp_dbh->date_in_context = 0;
    status = LIB$INIT_DATE_TIME_CONTEXT( &imp_dbh->date_in_context,
		    &LIB$K_INPUT_FORMAT, &format_dsc );
    if ( !( status & 1 ) ) do_error( dbh, 0, status );
    imp_dbh->date_out_context = 0;
    status = LIB$INIT_DATE_TIME_CONTEXT( &imp_dbh->date_out_context,
		    &LIB$K_OUTPUT_FORMAT, &format_dsc );
    if ( !( status & 1 ) ) do_error( dbh, 0, status );

    now_dsc.dsc$a_pointer = now_str;
    now_dsc.dsc$w_length = sizeof(now_str);
    now_dsc.dsc$b_dtype = DSC$K_DTYPE_T;
    now_dsc.dsc$b_class = DSC$K_CLASS_S;
    SYS$GETTIM( &now );
    LIB$FORMAT_DATE_TIME( &now_dsc, &now, &imp_dbh->date_out_context,
			  &imp_dbh->date_len, 0 );
    if (dbis->debug >= 2) {
	fprintf(DBILOGFP, "date_format max length is %d\n",
			  imp_dbh->date_len );
    }
}


int rdb_db_login( SV *dbh, imp_dbh_t *imp_dbh, char *db,
		  char *user, char *password )
{

    long status;
    char connection_name[32];
    D_imp_drh_from_dbh;
    char user_buf[256];
    char pass_buf[256];

    imp_drh->next_connection++;
    sprintf( connection_name, "CON_%d", imp_drh->next_connection );

    strncpy( user_buf, user, sizeof(user_buf)-1 );
    strncpy( pass_buf, password, sizeof(pass_buf)-1 );

    if (dbis->debug >= 2) {
	fprintf(DBILOGFP, "user %s password %s\n", user, password );
    }
    DBDSQL_CONNECT_AUTH( &status, db, connection_name, user_buf, pass_buf );
    if ( status ) {
	do_error( dbh, status, 0 );
	return 0;
    } else {
	char *format_str;
	int format_len;

	imp_drh->current_connection = imp_drh->next_connection;
	imp_dbh->connection = imp_drh->next_connection;
	imp_dbh->statement_nr = 1;
	imp_dbh->cursor_nr = 1;
	format_str = "|!Y4!MN0!D0|!H04!M0!S0!C2|";
	format_len = strlen(format_str);
	set_date_format( dbh, imp_dbh, format_str, format_len );

	DBIc_ACTIVE_on(imp_dbh);
	return 1;
    }
    return ( !status ) ? 1 : 0;
}


static int rdb_set_connection( imp_dbh_t *imp_dbh )
{
    long status;
    D_imp_drh_from_dbh;

    if ( imp_drh->current_connection != imp_dbh->connection ) {
	char connection_name[32];
	
	sprintf( connection_name, "CON_%d", imp_dbh->connection );
	DBDSQL_SET_CONNECTION( &status, connection_name );
        if (dbis->debug >= 2) {
	    fprintf(DBILOGFP, "set connect %s, %d\n", connection_name,
                              status );
	}
	if ( !status ) {
	    imp_drh->current_connection = imp_dbh->connection;
	    return 1;
	} else
	    return 0;
    }
    return 1;
}	
    

int rdb_db_disconnect( SV *dbh, imp_dbh_t *imp_dbh )
{
    long status;
    D_imp_drh_from_dbh;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    DBDSQL_DISCONNECT( &status );
    if ( status ) {
	do_error( dbh, status, 0 );
	return 0;
    }
    imp_drh->current_connection = 0;
    return 1;
}

int rdb_discon_all( SV *drh, imp_drh_t *imp_drh )
{
    long status;

    DBDSQL_DISCONNECT_ALL( &status );
    if ( status ) {
	do_error( drh, status, 0 );
	return 0;
    }
    imp_drh->current_connection = 0;
    imp_drh->next_connection = 0;
    return 1;
}

void dbd_db_destroy( SV *dbh, imp_dbh_t *imp_dbh )
{
    
}


int rdb_db_commit( SV *dbh, imp_dbh_t *imp_dbh )
{
    long status;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    DBDSQL_COMMIT( &status );
    if ( status ) {
	do_error( dbh, status, 0 );
	return 0;
    }
    return 1;
}

int rdb_db_rollback( SV *dbh, imp_dbh_t *imp_dbh )
{
    long status;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    DBDSQL_ROLLBACK( &status );
    if ( status ) {
	do_error( dbh, status, 0 );
	return 0;
    }
    return 1;
}

int rdb_db_do( SV *dbh, imp_dbh_t *imp_dbh, char *statement )
{
    long status, count;
    struct {
	short len;
	char buf[2002];
    } var_stmt;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    strncpy ( var_stmt.buf, statement, sizeof(var_stmt.buf)-1 );
    var_stmt.len = strlen(var_stmt.buf);
    DBDSQL_DO( &status, &var_stmt );
    if ( status && status != 100 ) {
	do_error( dbh, status, 0 );
	return 0;
    } 
    if ( DBIc_is(imp_dbh,DBIcf_AutoCommit) ) {
	DBDSQL_COMMIT( &status );
	if ( status && status != SQLCODE_NOIMPTXN ) {
	    do_error( dbh, status, 0 );
	    return 0;
	}
    }
    return -1;
}



int rdb_db_STORE_attrib( SV *dbh, imp_dbh_t *imp_dbh, 
	                 SV *keysv, SV *valuesv )
{
    char *key, *val;
    STRLEN len;

    key = SvPV(keysv,len);
    val = SvPV(valuesv,len);
    if ( !strcmp( key, "PrintError" ) ) {
	if ( SvTRUE( valuesv ) ) {
	    DBIc_on( imp_dbh, DBIcf_PrintError );
	} else {    
	    DBIc_off( imp_dbh, DBIcf_PrintError );
	}
    } else if ( !strcmp( key, "RaiseError" ) ) {
	if ( SvTRUE( valuesv ) ) {
	    DBIc_on( imp_dbh, DBIcf_RaiseError );
	} else {    
	    DBIc_off( imp_dbh, DBIcf_RaiseError );
	}
    } else if ( !strcmp( key, "ChopBlanks" ) ) {
	if ( SvTRUE( valuesv ) ) {
	    DBIc_on( imp_dbh, DBIcf_ChopBlanks );
	} else {    
	    DBIc_off( imp_dbh, DBIcf_ChopBlanks );
	}
    } else if ( !strcmp( key, "AutoCommit" ) ) {
	if ( SvTRUE( valuesv ) ) {
	    DBIc_on( imp_dbh, DBIcf_AutoCommit );
	} else {    
	    DBIc_off( imp_dbh, DBIcf_AutoCommit );
	}
    } else if ( !strcmp( key, "DateFormat" ) || 
	        !strcmp( key, "rdb_dateformat" ) ) {
	int status;
	char *format_str;
	unsigned int format_len;

	if ( !SvPOK(valuesv) || !SvTRUE(valuesv) ) {
	    warn( "setting date_format to default: |!Y4!MN0!D0|!H04!M0!S0!C2|" );
	    format_str = "|!Y4!MN0!D0|!H04!M0!S0!C2|";
	    format_len = strlen(format_str);
	} else {
	    format_str = SvPV( valuesv, format_len );
	}
	set_date_format( dbh, imp_dbh, format_str, format_len );
    }
    return 1;
}

SV* rdb_db_FETCH_attrib( SV *dbh, imp_dbh_t *imp_dbh, SV *keysv )
{
    char *key;
    STRLEN len;

    key = SvPV(keysv,len);
    if ( !strcmp( key, "PrintError" ) ) {
	if ( DBIc_is(imp_dbh,DBIcf_PrintError) ) {
	    return &PL_sv_yes;
	} else {    
	    return &PL_sv_no;
	}
    } else if ( !strcmp( key, "RaiseError" ) ) {
	if ( DBIc_is(imp_dbh,DBIcf_RaiseError) ) {
	    return &PL_sv_yes;
	} else {    
	    return &PL_sv_no;
	}
    } else if ( !strcmp( key, "ChopBlanks" ) ) {
	if ( DBIc_is(imp_dbh,DBIcf_ChopBlanks) ) {
	    return &PL_sv_yes;
	} else {    
	    return &PL_sv_no;
	}
    } else if ( !strcmp( key, "AutoCommit" ) ) {
	if ( DBIc_is(imp_dbh,DBIcf_AutoCommit) ) {
	    return &PL_sv_yes;
	} else {    
	    return &PL_sv_no;
	}
    } else if ( !strcmp( key, "DateFormat" ) || 
	        !strcmp( key, "rdb_dateformat" ) ) {
	return newSVpv( imp_dbh->date_format, 0 );
    } else {
	return &PL_sv_undef;
    }
}


void prep_sqlda( imp_sth_t *imp_sth, sql_t_sqlda2 **out, sql_t_sqlda2 *in )
{
    int i, sqlda_size;
    int data_type;
    void *p;
    sql_t_sqlvar2 *sqlpar;
    
    sqlda_size = sizeof(sql_t_sqlda2);
    if (in->sqld > 1 )
	sqlda_size +=  (in->sqld-1)*sizeof(sql_t_sqlvar2);
    *out = (sql_t_sqlda2 *) malloc( sqlda_size );
    memcpy( *out, in, sqlda_size );
    (*out)->sqln = in->sqld;

    if ( imp_sth ) {
	imp_sth->original_type = calloc( (*out)->sqld,
			                 sizeof(imp_sth->original_type[0]) );
	imp_sth->original_size = calloc( (*out)->sqld,
			                 sizeof(imp_sth->original_size[0]) );
    }
    for ( i = 0; i < (*out)->sqld; i++ ) {
	sqlpar = (*out)->sqlvar + i;
	/*
	**  in perl some datatypes are represented as strings, 
	**  so alloc enough room for string representation and 
	**  change sqlpar datatype saving the original. This happens
	**  only for a "select" sqlda structure
	*/
	if ( imp_sth ) {
	    imp_sth->original_type[i] = sqlpar->sqltype;
	    imp_sth->original_size[i] = sqlpar->sqloctet_len;
	}
	if ( ( ( sqlpar->sqltype == SQLDA_TINYINT ||
	         sqlpar->sqltype == SQLDA_SMALLINT ||
	         sqlpar->sqltype == SQLDA_INTEGER ) && sqlpar->sqlprcsn ) ||
             sqlpar->sqltype == SQLDA_QUADWORD ) {

	    sqlpar->sqllen = sqlpar->sqloctet_len = 32;
	    sqlpar->sqltype = SQLDA_VARCHAR;
	}
	sqlpar->sqldata = calloc ( 1, sqlpar->sqloctet_len + sizeof(int) );
	sqlpar->sqlind = (int *) ( sqlpar->sqldata + sqlpar->sqloctet_len );
	sqlpar->sqlname[ sqlpar->sqlname_len ] = 0;

        if (dbis->debug >= 2) {
	    fprintf(DBILOGFP, "par %d name %s type %d precision %d size %d\n", 
	        i, 
		sqlpar->sqlname,
		( imp_sth ) ? imp_sth->original_type[i] :
			      sqlpar->sqltype,
		sqlpar->sqlprcsn,
		( imp_sth ) ? imp_sth->original_size[i] :
			      sqlpar->sqloctet_len );
	}
    }
}

int rdb_st_prepare( SV *sth, imp_sth_t *imp_sth, char *statement, SV *pattribs)
{
    long status;
    int len;
    sql_t_sqlda2 *in_sqlda  = (sql_t_sqlda2 *)0;
    sql_t_sqlda2 *out_sqlda = (sql_t_sqlda2 *)0;
    sql_t_sqlca sqlca;
    D_imp_dbh_from_sth;
    HV *attribs;
    SV **pvalue;
    struct {
	short len;
	char buf[2002];
    } var_stmt;


    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    in_sqlda = __ALLOCA( sizeof(sql_t_sqlda2) + 254*sizeof(sql_t_sqlvar2) );
    memset (in_sqlda, 0, sizeof(sql_t_sqlda2) + 254*sizeof(sql_t_sqlvar2) );
    in_sqlda->sqln = 255;
    memcpy( in_sqlda->sqldaid, "SQLDA2  ", 8 );

    out_sqlda = __ALLOCA( sizeof(sql_t_sqlda2) + 254*sizeof(sql_t_sqlvar2) );
    memset (out_sqlda, 0, sizeof(sql_t_sqlda2) + 254*sizeof(sql_t_sqlvar2) );
    out_sqlda->sqln = 255;
    memcpy( out_sqlda->sqldaid, "SQLDA2  ", 8 );

    len = strlen(statement);
    if ( len > 2000 ) len = 2000;
    strncpy( var_stmt.buf, statement, len );
    var_stmt.len = len;

    if ( pattribs && SvROK(pattribs) && SvTYPE( SvRV(pattribs) ) ) {
	attribs = (HV *)SvRV(pattribs);
	if ( hv_exists(attribs,"rdb_hold",8) ) {
	    pvalue = hv_fetch(attribs,"rdb_hold",8,0);
	    if ( SvTRUE( *pvalue ) ) {
		imp_sth->hold = 1;
	    } else {
		imp_sth->hold = 0;
	    }
        } 
    }	

    sprintf( imp_sth->stmt_name, "STM_%d", imp_dbh->statement_nr++ );

    DBDSQL_PREPARE( &status, &var_stmt, imp_sth->stmt_name );
    if ( status ) {
	do_error( sth, status, 0 );
	return 0;
    }

    DBDSQL_DESCRIBE_MARKERS( &sqlca, imp_sth->stmt_name, in_sqlda );
    if ( sqlca.sqlcode ) {
	do_error( sth, sqlca.sqlcode, 0 );
	return 0;
    }
    prep_sqlda( (imp_sth_t *)0, &(imp_sth->in_sqlda), in_sqlda );
    DBIc_NUM_PARAMS(imp_sth) = in_sqlda->sqld;

    DBDSQL_DESCRIBE_SELECT( &sqlca, imp_sth->stmt_name, out_sqlda );
    if ( sqlca.sqlcode ) {
	do_error( sth, sqlca.sqlcode, 0 );
	return 0;
    }
    prep_sqlda( imp_sth, &(imp_sth->out_sqlda), out_sqlda );
    if ( DBIc_NUM_FIELDS(imp_sth) = out_sqlda->sqld ) {
	sprintf( imp_sth->cur_name, "CUR_%d", imp_dbh->cursor_nr++ );
	if ( imp_sth->hold ) {
	    DBDSQL_DECLARE_CURSOR_HOLD( &status, imp_sth->cur_name, imp_sth->stmt_name );
	} else {
	    DBDSQL_DECLARE_CURSOR( &status, imp_sth->cur_name, imp_sth->stmt_name );
	}
	if ( sqlca.sqlcode ) {  
	    do_error( sth, sqlca.sqlcode, 0 );
	    return 0;
	}
    }	
    if (dbis->debug >= 2) {
	fprintf(DBILOGFP, "num-par %d , num-sel %d\n",
                in_sqlda->sqld, out_sqlda->sqld );
    }


    DBIc_IMPSET_on(imp_sth);
    return 1;

}


static void free_sqlda( sql_t_sqlda2 *sqlda )
{
    int i;

    for ( i = 0; i < sqlda->sqld; i++ ) {
	if ( sqlda->sqlvar[i].sqldata ) 
	    free( sqlda->sqlvar[i].sqldata );
    }
    free( sqlda );    
}


static void print_sqlda( sql_t_sqlda2 *sqlda )
{
    int i, j;
    sql_t_sqlvar2 *sqlpar;

    for ( i = 0; i < sqlda->sqld; i++ ) {
	sqlpar = sqlda->sqlvar + i;
	fprintf(DBILOGFP, "par %d name %s type %d precision %d size %d|", 
		i, 
		sqlpar->sqlname,
		sqlpar->sqltype,
		sqlpar->sqlprcsn,
		sqlpar->sqloctet_len );
        for ( j = 0; j < sqlpar->sqloctet_len; j++ ) {
	    if ( isprint( sqlpar->sqldata[j] ) )
		fputc( sqlpar->sqldata[j] , DBILOGFP );
            else
	        fputc( '.', DBILOGFP );
        }
        fprintf( DBILOGFP, "|\n" );
    }
}


int rdb_st_execute( SV *sth, imp_sth_t *imp_sth )
{
    long status;
    D_imp_dbh_from_sth;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    if ( DBIc_NUM_FIELDS(imp_sth) ) {
	if ( dbis->debug >= 4 ) print_sqlda( imp_sth->in_sqlda );
	DBDSQL_OPEN_CURSOR( &status, imp_sth->cur_name, imp_sth->in_sqlda );
	if ( status ) {
	    do_error( sth, status, 0 );
	    return 0;
	}
    } else {
	if ( dbis->debug >= 4 ) print_sqlda( imp_sth->in_sqlda );
	DBDSQL_EXECUTE( &status, imp_sth->stmt_name, imp_sth->in_sqlda );
	if ( status ) {
	    do_error( sth, status, 0 );
	    return 0;
	}
	if ( DBIc_is(imp_dbh,DBIcf_AutoCommit) ) {
	    DBDSQL_COMMIT( &status );
	    if ( status && status != SQLCODE_NOIMPTXN ) {
		do_error( sth, status, 0 );
		return 0;
	    }
	}
    }    
    DBIc_ACTIVE_on(imp_sth);
    return -1;
}


void rdb_st_destroy( SV *sth, imp_sth_t *imp_sth )
{
    long status;
    int i;

    if ( imp_sth ) {
	if ( imp_sth->stmt_name ) {
	    DBDSQL_RELEASE( &status, (char *)imp_sth->stmt_name );
	    if ( status ) {
		do_error( sth, status, 0 );
	    }
	}
	if ( imp_sth->in_sqlda ) 
	    free_sqlda( imp_sth->in_sqlda );
	if ( imp_sth->out_sqlda ) 
	    free_sqlda( imp_sth->out_sqlda );
	imp_sth->in_sqlda = ( sql_t_sqlda2 *)0;
	imp_sth->out_sqlda = ( sql_t_sqlda2 *)0;
	if ( imp_sth->original_type )
	    free( imp_sth->original_type );
	if ( imp_sth->original_size )
	    free( imp_sth->original_size );
	DBIc_IMPSET_off(imp_sth);
    }
}

int rdb_st_finish( SV *sth, imp_sth_t *imp_sth )
{

    long status;
    D_imp_dbh_from_sth;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    if ( dbis->debug >= 4 ) {
	fprintf( DBILOGFP, "finish %s %s\n", imp_sth->stmt_name, 
                 imp_sth->cur_name );
    }

    DBIc_ACTIVE_off(imp_sth);
    if ( DBIc_NUM_FIELDS(imp_sth) ) {
	    DBDSQL_CLOSE_CURSOR( &status, imp_sth->cur_name );
	    if ( status ) {
		do_error( sth, status, 0 );
		return 0;
	    }
    }
    return 1;
}

int rdb_st_blob_read( SV *sth, imp_sth_t *imp_sth,
                      int field, long offset, long len, 
		      SV *destrv, long destoffset )
{
    return -2;
}

AV *dbd_st_fetch( SV *sth, imp_sth_t *imp_sth )
{
    D_imp_dbh_from_sth;
    AV *av;
    int num_fields;
    int chop_blanks;
    int int_val, i;
    double double_val;
    STRLEN len;
    long status;
    sql_t_sqlvar2 *sqlpar;
    sql_t_varchar *varchar;
    char time_buf[32];
    struct dsc$descriptor_s time_dsc = {
	sizeof(time_buf), DSC$K_DTYPE_T, DSC$K_CLASS_S, (char *)0 };

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    if ( !DBIc_is(imp_sth,DBIcf_ACTIVE) ) {
	return Nullav;
    }

    num_fields = DBIc_NUM_FIELDS(imp_sth);
    chop_blanks = DBIc_is( imp_sth, DBIcf_ChopBlanks );

    if ( dbis->debug >= 4 ) {
	fprintf( DBILOGFP, "%s\n", imp_sth->stmt_name );
	fprintf( DBILOGFP, "cursor %s\n", imp_sth->cur_name );
	print_sqlda( imp_sth->out_sqlda );
    }

    DBDSQL_FETCH_CURSOR( &status, imp_sth->cur_name, imp_sth->out_sqlda );
    if ( status ) {
	if ( status == 100 ) {
            rdb_st_finish( sth, imp_sth );
	    if ( DBIc_is(imp_dbh,DBIcf_AutoCommit) ) {
		DBDSQL_COMMIT( &status );
		if ( status && status != SQLCODE_NOIMPTXN ) {
		    do_error( imp_dbh->dbh, status, 0 );
		    return Nullav;
		}
	    }
	    return Nullav;
	} else {
	    do_error( sth, status, 0 );
	    return Nullav;
	}
    }	
    av = DBIS->get_fbav(imp_sth);
    for ( i = 0; i < num_fields; i++ ) {
	sqlpar = imp_sth->out_sqlda->sqlvar + i;

	if ( !*( sqlpar->sqlind ) ) {
	    //
	    // not NULL
	    //
	    switch( sqlpar->sqltype ) {
		case SQLDA_VARCHAR:
		case SQLDA_VARBYTE:
		    varchar = (sql_t_varchar *) sqlpar->sqldata;
		    for ( ; 
			  varchar->len && 
                          ( chop_blanks || 
			    imp_sth->original_type[i] != SQLDA_VARCHAR ) && 
			  varchar->buf[varchar->len-1] == ' '; 
			  varchar->len-- );
		    sv_setpvn( AvARRAY(av)[i], varchar->buf, varchar->len );
		    break;		
		case SQLDA_CHAR:
		    for ( len = sqlpar->sqllen; 
			  len && chop_blanks && sqlpar->sqldata[len-1] == ' '; 
			  len-- );
		    sv_setpvn( AvARRAY(av)[i], sqlpar->sqldata, len );
		    break;
		case SQLDA_TINYINT:
		    int_val = *( (char *) sqlpar->sqldata );
		    goto int_common;
		case SQLDA_SMALLINT:
		    int_val = *( (short *) sqlpar->sqldata );
		    goto int_common;
		case SQLDA_INTEGER:
		    int_val = *( (int *) sqlpar->sqldata );
		    int_common:
		    sv_setiv( AvARRAY(av)[i], int_val );
		    break;
		case SQLDA_FLOAT:
		    double_val = *( (double *) sqlpar->sqldata );
		    sv_setnv( AvARRAY(av)[i], double_val );
		    break;		
		case SQLDA_DATE:
		case SQLDA2_DATETIME:
		    time_dsc.dsc$a_pointer = time_buf;
		    time_dsc.dsc$w_length = sizeof(time_buf);
		    status = LIB$FORMAT_DATE_TIME ( &time_dsc, 
						    (int64 *) sqlpar->sqldata, 
						    &imp_dbh->date_out_context,
						    &time_dsc.dsc$w_length,
						    0 );
		    if ( !( status & 1) ) {
			do_error( sth, 0, status );
			time_dsc.dsc$w_length = 0;
		    }
		    sv_setpvn( AvARRAY(av)[i], time_dsc.dsc$a_pointer,
			       time_dsc.dsc$w_length );
		    break;		
	    }
	} else {
	    //
	    //  NULL value
	    //
	    sv_setsv( AvARRAY(av)[i], &sv_undef );
	}
    }				
    

    return av;
}    



int rdb_st_STORE_attrib( SV *sth, imp_sth_t *imp_sth, 
	                 SV *keysv, SV *valuesv )
{
    printf( "st_store\n" );
    return 1;
}

SV* rdb_st_FETCH_attrib( SV *sth, imp_sth_t *imp_sth, 
	                 SV *keysv )
{
    D_imp_dbh_from_sth;
    char *key;
    STRLEN len;
    int i;
    SV *val;    
    AV *av;

    key = SvPV(keysv,len);
    if ( !strcmp( key, "CursorName" ) && imp_sth->cur_name[0] ) {
	val = sv_2mortal( newSVpv( imp_sth->cur_name, 0 ) );
    } else if ( !strcmp( key, "NUM_OF_FIELDS" ) ) {
	val = sv_2mortal( newSViv( DBIc_NUM_FIELDS(imp_sth) ) );
    } else if ( !strcmp( key, "NUM_OF_PARAMS" ) ) {
	val = sv_2mortal( newSViv( DBIc_NUM_PARAMS(imp_sth) ) );
    } else if ( !strcmp( key, "Active" ) ) {
	val = sv_2mortal( newSViv( DBIc_is(imp_sth,DBIcf_ACTIVE) ) );
    } else if ( !strcmp ( key, "NAME" ) ||
	        !strcmp ( key, "NAME_lc" ) ||
		!strcmp ( key, "NAME_uc" ) ) {
	for ( av = newAV(), i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++ ) {
	    if ( !strcmp( key, "NAME_lc" ) ) {
		char *p;
    
		for ( p = imp_sth->out_sqlda->sqlvar[i].sqlname;
		      *p; p++ ) *p = tolower(*p);
	    } else {
		char *p;
    
		for ( p = imp_sth->out_sqlda->sqlvar[i].sqlname;
		      *p; p++ ) *p = toupper(*p);
	    }
	    val = newSVpv(imp_sth->out_sqlda->sqlvar[i].sqlname, 0 );
	    if ( !av_store( av, i, val ) )
		SvREFCNT_dec(val);
	}
	val = newRV_inc( (SV*) av );
    } else if ( !strcmp ( key, "TYPE" ) ) {
	for ( av = newAV(), i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++ ) {
	    val = newSViv(imp_sth->original_type[i] );
	    if ( !av_store( av, i, val ) )
		SvREFCNT_dec(val);
	}
	val = newRV_inc( (SV*) av );
    } else if ( !strcmp ( key, "SCALE" ) ) {
	for ( av = newAV(), i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++ ) {
	    switch ( imp_sth->original_type[i] ) {
		case SQLDA_INTEGER:
		case SQLDA_FLOAT:
		case SQLDA_SMALLINT:
		case SQLDA_QUADWORD:
		    val = newSViv( imp_sth->out_sqlda->sqlvar[i].sqlprcsn );
		    break;
		default:
		    val = &sv_undef;
		    break;
	    }
	    if ( !av_store( av, i, val ) )
		SvREFCNT_dec(val);
	}
	val = newRV_inc( (SV*) av );
    } else if ( !strcmp ( key, "NULLABLE" ) ) {
	for ( av = newAV(), i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++ ) {
	    val = newSViv( 2 );
	    if ( !av_store( av, i, val ) )
		SvREFCNT_dec(val);
	}
	val = newRV_inc( (SV*) av );
    } else if ( !strcmp ( key, "PRECISION" ) ) {
	for ( av = newAV(), i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++ ) {
	    int prec;

	    switch ( imp_sth->original_type[i] ) {

		case SQLDA_VARBYTE:
		case SQLDA_VARCHAR:
		    prec = imp_sth->original_size[i] - sizeof(int); 
		    // assuming sql_t_varchar
		    break;
		case SQLDA_CHAR:
		    prec = imp_sth->original_size[i];
		    break;
		case SQLDA_FLOAT:
		    // assuming IEEE_FLOAT
		    prec = 17;
		    break;
		    break;
		case SQLDA_INTEGER:
		case SQLDA_SMALLINT:
		case SQLDA_QUADWORD:
		    switch( imp_sth->original_size[i] ) {
			case 1:
			    prec = 2;
			    break;
			case 2:
			    prec = 4;
			    break;
			case 4:
			    prec = 9;
			    break;
			case 8:
			    prec = 18;
			    break;
			default:
			    die( "unknown integer data size" );
			    break;
		    }			    
		    break;
		case SQLDA_DATE:
		case SQLDA2_DATETIME:
		    prec = imp_dbh->date_len;
		    break;
		default:
		    val = 0;
	    }
	    val = newSViv( prec );
	    if ( !av_store( av, i, val ) )
		SvREFCNT_dec(val);
	}
	val = newRV_inc( (SV*) av );
    } else {
	do_error( sth, 0, SS$_NOSUCHOBJ );
	val = &sv_undef;
    }	

    return val;
}

int rdb_bind_ph ( SV *sth, imp_sth_t *imp_sth,
                  SV *par_name, SV *par_value, IV sql_type, 
                  SV *attribs,
		  int is_inout, IV maxlen)
{
    int par_idx, int_val, status;
    double double_val;
    char *p;
    STRLEN len;
    sql_t_sqlvar2 *sqlpar;
    struct dsc$descriptor_s text_dsc;
    sql_t_varchar *varchar;
    D_imp_dbh_from_sth;

    if (SvGMAGICAL(par_name))
        mg_get(par_name);
    if ( SvNIOKp(par_name) )
	par_idx = (int)SvIV(par_name);
    else
	do_error( sth, 0, SS$_ABORT );

    if (dbis->debug >= 2) {
	fprintf(DBILOGFP, "       bind %d <== %s (type %ld",
		par_idx, neatsvpv(par_value,0), (long)sql_type);
	if (is_inout)
	    fprintf(DBILOGFP, ", inout 0x%lx, maxlen %ld",
		(long)par_value, (long)maxlen);
	if (attribs)
	    fprintf(DBILOGFP, ", attribs: %s", neatsvpv(attribs,0));
	fprintf(DBILOGFP, ")\n");
    }

    if ( !par_idx || par_idx > imp_sth->in_sqlda->sqld )
	do_error( sth, 0, SS$_ABORT );

    sqlpar = imp_sth->in_sqlda->sqlvar + ( par_idx - 1 );

    if (SvGMAGICAL(par_value))
        mg_get(par_value);

    if ( !SvOK(par_value) ) {
	//
	//  parameter value is undef -> store NULL
	//
	*(sqlpar->sqlind) = -1;
    } else {
	*(sqlpar->sqlind) = 0;
	switch ( sqlpar->sqltype ) {
	    case SQLDA_VARCHAR:
	    case SQLDA_VARBYTE:
		    p = SvPV(par_value,len);
		    varchar = (sql_t_varchar *) sqlpar->sqldata;
		    memcpy( varchar->buf, p, sqlpar->sqllen );
		    varchar->len = len;
		    break;
	    case SQLDA_CHAR:
		    p = SvPV(par_value,len);
		    memcpy( sqlpar->sqldata, p, sqlpar->sqllen );
		    while ( len < sqlpar->sqllen ) {
			sqlpar->sqldata[len++] = ' ';
		    }
		    break;
	    case SQLDA_TINYINT:
		    int_val = SvIV(par_value);
		    * ( (char *)sqlpar->sqldata ) = int_val;
		    break;
	    case SQLDA_SMALLINT:
		    int_val = SvIV(par_value);
		    * ( (short *)sqlpar->sqldata ) = int_val;
		    break;
	    case SQLDA_INTEGER:
		    int_val = SvIV(par_value);
		    * ( (int *)sqlpar->sqldata ) = int_val;
		    break;
	    case SQLDA_FLOAT:
		    double_val = SvNV(par_value);
		    * ( (double *)sqlpar->sqldata ) = double_val;
		    break;		
	    case SQLDA_DATE:
	    case SQLDA2_DATETIME:
		    p = SvPV(par_value,len);
		    text_dsc.dsc$b_dtype = DSC$K_DTYPE_T;
		    text_dsc.dsc$b_class = DSC$K_CLASS_S;
		    text_dsc.dsc$a_pointer = p;
		    text_dsc.dsc$w_length = len;
		    *(int64 *)(sqlpar->sqldata) = 0;
		    status = LIB$CONVERT_DATE_STRING( &text_dsc, 
                                    sqlpar->sqldata, 
				    &imp_dbh->date_in_context, 0, 0, 0 );
		    if ( !( status & 1 ) ) 
			do_error( sth, 0, status  );
		    break;
	    default:
		    do_error( sth, 0, SS$_ABORT );
		    break;
	}
    }				
    return 1;
}



