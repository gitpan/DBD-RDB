#include <descrip>
#include <builtins>
#include <limits.h>
#include <lib$routines>
#include <sql_literals>
#include <ints>
#include <libdtdef>
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


static int do_error( SV *h, long status, char *what )
{
    char err_buf[1024];
    int err_txtlen;
    extern int SQL_GET_ERROR_TEXT();

    D_imp_xxh(h);
    SV *errstr = DBIc_ERRSTR(imp_xxh);

    if ( status ) {
	SQL_GET_ERROR_TEXT( err_buf, sizeof(err_buf)-1, &err_txtlen );
	err_buf[err_txtlen] = 0;
	sv_setpv( errstr, err_buf );
	sv_setiv( DBIc_ERR(imp_xxh), (IV)status );
        DBIh_EVENT2( h, ERROR_event, DBIc_ERR(imp_xxh), errstr );
    } else if ( what ) {
	sv_setpv( errstr, what );
	sv_setiv( DBIc_ERR(imp_xxh), 999 );
        DBIh_EVENT2( h, ERROR_event, DBIc_ERR(imp_xxh), errstr );
    }    
    return 0;
}


int rdb_db_login( SV *dbh, imp_dbh_t *imp_dbh, char *db,
		  char *user, char *password )
{

    long status;
    char connection_name[32];
    D_imp_drh_from_dbh;

    imp_drh->next_connection++;
    sprintf( connection_name, "CON_%d", imp_drh->next_connection );
    DBDSQL_CONNECT( &status, db, connection_name );
    if ( status ) {
	do_error( dbh, status, NULL );
	return 0;
    } else {
	imp_drh->current_connection = imp_drh->next_connection;
	imp_dbh->connection = imp_drh->next_connection;
	imp_dbh->statement_nr = 1;
	imp_dbh->cursor_nr = 1;
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
	do_error( dbh, status, NULL );
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
	do_error( drh, status, NULL );
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
	do_error( dbh, status, NULL );
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
	do_error( dbh, status, NULL );
	return 0;
    }
    return 1;
}

int rdb_db_do( SV *dbh, imp_dbh_t *imp_dbh, char *statement )
{
    long status;
    struct {
	short len;
	char buf[2000];
    } var_stmt;

    if ( !rdb_set_connection( imp_dbh ) ) return 0;

    strncpy ( var_stmt.buf, statement, sizeof(var_stmt.buf)-1 );
    var_stmt.len = strlen(var_stmt.buf);
    DBDSQL_DO( &status, &var_stmt );
    if ( status ) {
	do_error( dbh, status, NULL );
	return 0;
    }
    if ( DBIc_is(imp_dbh,DBIcf_AutoCommit) ) {
	DBDSQL_COMMIT( &status );
	if ( status && status != SQLCODE_NOIMPTXN ) {
	    do_error( dbh, status, NULL );
	    return 0;
	}
    }
    return 1;
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
    } else {
	return &PL_sv_undef;
    }
}


void prep_sqlda( sql_t_sqlda2 **out, sql_t_sqlda2 *in )
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
    for ( i = 0; i < (*out)->sqld; i++ ) {
	sqlpar = (*out)->sqlvar + i;
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
		sqlpar->sqltype,
		sqlpar->sqlprcsn,
		sqlpar->sqloctet_len );
	}
    }
}

int rdb_st_prepare( SV *sth, imp_sth_t *imp_sth, char *statement, SV *pattribs)
{
    long status;
    int len;
    sql_t_varchar_w *var_stmt;
    sql_t_sqlda2 *in_sqlda  = (sql_t_sqlda2 *)0;
    sql_t_sqlda2 *out_sqlda = (sql_t_sqlda2 *)0;
    sql_t_sqlca sqlca;
    D_imp_dbh_from_sth;
    HV *attribs;
    SV **pvalue;

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
    var_stmt = (sql_t_varchar_w *) malloc( len + sizeof(sql_t_varchar) );
    strncpy( var_stmt->buf, statement, len );
    var_stmt->buf[len] = 0;
    var_stmt->len = len;
    imp_sth->stmt = var_stmt;

    sprintf( imp_sth->stmt_name, "STM_%d", imp_dbh->statement_nr++ );

    DBDSQL_PREPARE( &status, imp_sth->stmt, imp_sth->stmt_name );
    if ( status ) {
	do_error( sth, status, NULL );
	return 0;
    }

    DBDSQL_DESCRIBE_MARKERS( &sqlca, imp_sth->stmt_name, in_sqlda );
    if ( sqlca.sqlcode ) {
	do_error( sth, sqlca.sqlcode, NULL );
	return 0;
    }
    prep_sqlda( &(imp_sth->in_sqlda), in_sqlda );
    DBIc_NUM_PARAMS(imp_sth) = in_sqlda->sqld;

    DBDSQL_DESCRIBE_SELECT( &sqlca, imp_sth->stmt_name, out_sqlda );
    if ( sqlca.sqlcode ) {
	do_error( sth, sqlca.sqlcode, NULL );
	return 0;
    }
    prep_sqlda( &(imp_sth->out_sqlda), out_sqlda );
    if ( DBIc_NUM_FIELDS(imp_sth) = out_sqlda->sqld ) {
	sprintf( imp_sth->cur_name, "CUR_%d", imp_dbh->cursor_nr++ );
	if ( imp_sth->hold ) {
	    DBDSQL_DECLARE_CURSOR_HOLD( &status, imp_sth->cur_name, imp_sth->stmt_name );
	} else {
	    DBDSQL_DECLARE_CURSOR( &status, imp_sth->cur_name, imp_sth->stmt_name );
	}
	if ( sqlca.sqlcode ) {  
	    do_error( sth, sqlca.sqlcode, NULL );
	    return 0;
	}
    }	
    if (dbis->debug >= 2) {
	fprintf(DBILOGFP, "num-par %d , num-sel %d\n",
                in_sqlda->sqld, out_sqlda->sqld );
    }

    if ( pattribs && SvROK(pattribs) && SvTYPE( SvRV(pattribs) ) ) {
	attribs = (HV *)SvRV(pattribs);
	if ( hv_exists(attribs,"Hold",4) ) {
	    pvalue = hv_fetch(attribs,"Hold",4,0);
	    if ( SvTRUE( *pvalue ) ) {
		imp_sth->hold = 1;
	    } else {
		imp_sth->hold = 0;
	    }
        } 
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
	    do_error( sth, status, NULL );
	    return 0;
	}
    } else {
	if ( dbis->debug >= 4 ) print_sqlda( imp_sth->in_sqlda );
	DBDSQL_EXECUTE( &status, imp_sth->stmt_name, imp_sth->in_sqlda );
	if ( status ) {
	    do_error( sth, status, NULL );
	    return 0;
	}
    }    
    DBIc_ACTIVE_on(imp_sth);
    return 1;
}


void rdb_st_destroy( SV *sth, imp_sth_t *imp_sth )
{
    int i;

    if ( imp_sth ) {
	if ( imp_sth->in_sqlda ) 
	    free_sqlda( imp_sth->in_sqlda );
	if ( imp_sth->out_sqlda ) 
	    free_sqlda( imp_sth->out_sqlda );
	imp_sth->in_sqlda = ( sql_t_sqlda2 *)0;
	imp_sth->out_sqlda = ( sql_t_sqlda2 *)0;
	if ( imp_sth->stmt )
	    free( imp_sth->stmt );
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
		do_error( sth, status, NULL );
		return 0;
	    }
    }
    return 1;
}

int rdb_st_blob_read( SV *sth, imp_sth_t *imp_sth,
                      int field, long offset, long len, 
		      SV *destrv, long destoffset )
{
    return 0;
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
	fprintf( DBILOGFP, "%s %s\n", imp_sth->stmt_name, imp_sth->stmt->buf );
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
		    do_error( imp_dbh->dbh, status, NULL );
		    return Nullav;
		}
	    }
	    return Nullav;
	} else {
	    do_error( sth, status, NULL );
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
			  varchar->len && chop_blanks && varchar->buf[varchar->len-1] == ' '; 
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
		    status = LIB$FORMAT_DATE_TIME ( &time_dsc, 
						    (int64 *) sqlpar->sqldata, 0,
						    &time_dsc.dsc$w_length, 0 );
		    if ( !( status & 1) ) {
			croak( "date conversion failed after fetch" );
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
    char *key;
    STRLEN len;
    int i;
    SV *val;    
    AV *av;

    key = SvPV(keysv,len);
    if ( !strcmp( key, "CursorName" ) && imp_sth->cur_name[0] ) {
	val = sv_2mortal( newSVpv( imp_sth->cur_name, 0 ) );
    } else if ( !strcmp( key, "Statement" ) && imp_sth->stmt->len ) {
	val = sv_2mortal( newSVpv( imp_sth->stmt->buf, 0 ) );
    } else if ( !strcmp( key, "NUM_OF_FIELDS" ) ) {
	val = sv_2mortal( newSViv( DBIc_NUM_FIELDS(imp_sth) ) );
    } else if ( !strcmp( key, "Active" ) ) {
	val = sv_2mortal( newSViv( DBIc_is(imp_sth,DBIcf_ACTIVE) ) );
    } else if ( !strcmp( key, "NAME" ) ) {
	for ( av = newAV(), i = 0; i < DBIc_NUM_FIELDS(imp_sth); i++ ) {
	    val = newSVpv(imp_sth->out_sqlda->sqlvar[i].sqlname, 0 );
	    if ( !av_store( av, i, val ) )
		SvREFCNT_dec(val);
	}
	val = newRV_inc( (SV*) av );
    } else {
	croak( "invalid attribute name" );
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
    char *p;
    STRLEN len;
    sql_t_sqlvar2 *sqlpar;
    struct dsc$descriptor_s text_dsc;
    sql_t_varchar *varchar;
    D_imp_dbh_from_sth;
    static int date_context = 0;
    $DESCRIPTOR( date_dsc, "|!Y4!MN0!D0!H04:!M0:!S0!C2|" );

    if ( !date_context ) {
	status = LIB$INIT_DATE_TIME_CONTEXT( &date_context, 
                                             &LIB$K_INPUT_FORMAT,
                                             &date_dsc );
        if ( !(status & 1) ) croak( "LIB$INIT_DATE_TIME_CONTEXT failed" );
    }

    if (SvGMAGICAL(par_name))
        mg_get(par_name);
    if ( SvNIOKp(par_name) )
	par_idx = (int)SvIV(par_name);
    else
	croak( "parameter name is not a number" );

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
	croak( "parameter array boundary violation" );

    sqlpar = imp_sth->in_sqlda->sqlvar + ( par_idx - 1 );

    if (SvGMAGICAL(par_value))
        mg_get(par_value);

    if ( !SvOK(par_value) ) {
	//
	//  parameter value is undef -> store NULL
	//
	*(sqlpar->sqlind) = -1;
    } else {
	switch ( sqlpar->sqltype ) {
	    case SQLDA_VARCHAR:
	    case SQLDA_VARBYTE:
		    p = SvPV(par_value,len);
		    varchar = (sql_t_varchar *) sqlpar->sqldata;
		    strncpy( varchar->buf, p, sqlpar->sqllen );
		    varchar->len = len;
		    break;
	    case SQLDA_CHAR:
		    p = SvPV(par_value,len);
		    strncpy( sqlpar->sqldata, p, sqlpar->sqllen );
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
	    case SQLDA_DATE:
	    case SQLDA2_DATETIME:
		    p = SvPV(par_value,len);
		    text_dsc.dsc$b_dtype = DSC$K_DTYPE_T;
		    text_dsc.dsc$b_class = DSC$K_CLASS_S;
		    text_dsc.dsc$a_pointer = p;
		    text_dsc.dsc$w_length = len;
		    status = LIB$CONVERT_DATE_STRING( &text_dsc, 
                                    sqlpar->sqldata, 0, 0, 0, 0 );
		    if ( !( status & 1 ) ) 
			do_error( sth, 0, "date conversion failed" );
		    break;
	    default:
		    do_error( sth, 0, "unknown data type requested" );
		    break;
	}
    }				
    return 1;
}



