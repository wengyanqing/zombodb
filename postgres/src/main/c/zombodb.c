/*
 * Portions Copyright 2013-2015 Technology Concepts & Design, Inc
 * Portions Copyright 2015-2019 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include "am/zdbam.h"
#include "am/zdb_interface.h"
#include "util/curl_support.h"
#include "rest/rest.h"

// Add from GPDB
#include "access/heapam.h"
#include "access/htup_details.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "catalog/oid_dispatch.h"
#include "cdb/cdbutil.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(zdb_es_direct_request);
PG_FUNCTION_INFO_V1(zdb_create_access_method);

Datum zdb_es_direct_request(PG_FUNCTION_ARGS);
Datum zdb_create_access_method(PG_FUNCTION_ARGS);

/*
 * Library initialization functions
 */
void _PG_init(void);
void _PG_fini(void);

void _PG_init(void) {
    curl_support_init();

    zdbam_init();
}

void _PG_fini(void) {
    zdbam_fini();
}

Datum zdb_es_direct_request(PG_FUNCTION_ARGS) {
    Oid                indexrelid     = PG_GETARG_OID(0);
    char               *method        = GET_STR(PG_GETARG_TEXT_P(1));
    char               *endpoint      = GET_STR(PG_GETARG_TEXT_P(2));
    ZDBIndexDescriptor *desc;
    StringInfo         final_endpoint = makeStringInfo();
    StringInfo         response;

    desc = zdb_alloc_index_descriptor_by_index_oid(indexrelid);

    if (endpoint[0] == '/') {
        /* user wants to hit the cluster itself */
        appendStringInfo(final_endpoint, "%s%s", desc->url, endpoint+1);
    } else {
        /* user wants to hit the specific index */
        appendStringInfo(final_endpoint, "%s%s/%s", desc->url, desc->fullyQualifiedName, endpoint);
    }
    response = rest_call(method, final_endpoint->data, NULL, 0);
    freeStringInfo(final_endpoint);

    PG_RETURN_TEXT_P(cstring_to_text(response->data));
}

static
bool ZdbCreateAccessMethod(const char *amName,
						Oid aminsert,
						Oid ambeginscan,
						Oid amgettuple,
						Oid amgetbitmap,
						Oid amrescan,
						Oid amendscan,
						Oid ammarkpos,
						Oid amrestrpos,
						Oid ambuild,
						Oid ambuildempty,
						Oid ambulkdelete,
						Oid amvacuumcleanup,
						Oid amcanreturn,
						Oid amcostestimate,
						Oid amoptions) {
	Relation    rel;
	Datum       values[Natts_pg_am];
	bool        nulls[Natts_pg_am];
	HeapTuple   tup;
	Oid         amoid;
	char		*sql;

	rel = heap_open(AccessMethodRelationId, RowExclusiveLock);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[Anum_pg_am_amname - 1] 			= DirectFunctionCall1(namein, CStringGetDatum(amName));
	values[Anum_pg_am_amstrategies - 1]		= Int16GetDatum(1);
	values[Anum_pg_am_amsupport - 1]		= Int16GetDatum(1);
	values[Anum_pg_am_amcanorder - 1]		= BoolGetDatum(false);
	values[Anum_pg_am_amcanorderbyop - 1]	= BoolGetDatum(false);
	values[Anum_pg_am_amcanbackward - 1]	= BoolGetDatum(false);
	values[Anum_pg_am_amcanunique - 1]		= BoolGetDatum(false);
	values[Anum_pg_am_amcanmulticol - 1]	= BoolGetDatum(true);
	values[Anum_pg_am_amoptionalkey - 1]	= BoolGetDatum(false);
	values[Anum_pg_am_amsearcharray - 1]	= BoolGetDatum(false);

	values[Anum_pg_am_amsearchnulls - 1]	= BoolGetDatum(true);
	values[Anum_pg_am_amstorage - 1]		= BoolGetDatum(true);
	values[Anum_pg_am_amclusterable - 1]	= BoolGetDatum(false);
	values[Anum_pg_am_ampredlocks - 1]		= BoolGetDatum(false);
	values[Anum_pg_am_amkeytype - 1]		= ObjectIdGetDatum(InvalidOid);
	values[Anum_pg_am_aminsert - 1]			= ObjectIdGetDatum(aminsert);
	values[Anum_pg_am_ambeginscan - 1]		= ObjectIdGetDatum(ambeginscan); 
	values[Anum_pg_am_amgettuple - 1]		= ObjectIdGetDatum(amgettuple);
	values[Anum_pg_am_amgetbitmap - 1]		= ObjectIdGetDatum(amgetbitmap);
	values[Anum_pg_am_amrescan - 1]			= ObjectIdGetDatum(amrescan);

	values[Anum_pg_am_amendscan - 1]		= ObjectIdGetDatum(amendscan);
	values[Anum_pg_am_ammarkpos - 1]		= ObjectIdGetDatum(ammarkpos);
	values[Anum_pg_am_amrestrpos - 1]		= ObjectIdGetDatum(amrestrpos);
	values[Anum_pg_am_ambuild - 1]			= ObjectIdGetDatum(ambuild);
	values[Anum_pg_am_ambuildempty - 1]		= ObjectIdGetDatum(ambuildempty);
	values[Anum_pg_am_ambulkdelete - 1]		= ObjectIdGetDatum(ambulkdelete);
	values[Anum_pg_am_amvacuumcleanup - 1]	= ObjectIdGetDatum(amvacuumcleanup);
	values[Anum_pg_am_amcanreturn - 1]		= ObjectIdGetDatum(amcanreturn);
	values[Anum_pg_am_amcostestimate - 1]	= ObjectIdGetDatum(amcostestimate);
	values[Anum_pg_am_amoptions - 1]		= ObjectIdGetDatum(amoptions);

	tup = heap_formtuple(RelationGetDescr(rel), values, nulls);
	amoid = simple_heap_insert(rel, tup);
	heap_freetuple(tup);

	heap_close(rel, RowExclusiveLock);

	if (Gp_role == GP_ROLE_DISPATCH) {
		sql = psprintf("SELECT zdb_create_access_method('%s', '')", amName);		
		CdbDispatchCommandToSegmentsWithOids(sql, DF_CANCEL_ON_ERROR, cdbcomponent_getCdbComponentsList(), GetAssignedOidsForDispatch(), NULL);
	}

	return true;	
}

Datum zdb_create_access_method(PG_FUNCTION_ARGS) {
    char *name = GET_STR(PG_GETARG_TEXT_P(0));
    char *insertFunc = GET_STR(PG_GETARG_TEXT_P(1));
	elog(NOTICE, "zdb_create_access_method: [name:%s insert:%s]", name, insertFunc);

	ZdbCreateAccessMethod(name,
						25438,	// insert
						25439,	// beginscan
						25440,	// gettuple
						InvalidOid,	// getbitmap
						25441,	// rescan
						25442,	// endscan
						25443,	// markpos
						25444,	// restrpos
						25445,	// build
						25446,	// buildempty
						25447,	// bulkdelete
						25448,	// vacuumcleanup
						InvalidOid,	// canreturn
						25449,	// costestimate
						25451);	// options

	PG_RETURN_INT32(0);
}
