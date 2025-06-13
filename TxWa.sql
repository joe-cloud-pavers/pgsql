SELECT
    -- Select the common key, coalescing to handle NULLs from the outer join
    COALESCE(mdm.prov_loc_speclty_key, src.prov_loc_speclty_key) AS common_prov_loc_speclty_key,

    -- Determine existence status based on the common key
    CASE
        WHEN mdm.prov_loc_speclty_key IS NOT NULL AND src.prov_loc_speclty_key IS NOT NULL THEN 'Matched in Both'
        WHEN mdm.prov_loc_speclty_key IS NOT NULL THEN 'Only in MDM_PROV_LOC_SPECLTY'
        WHEN src.prov_loc_speclty_key IS NOT NULL THEN 'Only in SRC'
        ELSE 'Not exists in any one' -- This case implies an issue if prov_loc_speclty_key could be NULL in either, otherwise shouldn't occur
    END AS prov_loc_speclty_key_existence_status,

    -- Comparison for 'speclty_key'
    CASE
        WHEN mdm.prov_loc_speclty_key IS NOT NULL AND src.prov_loc_speclty_key IS NOT NULL THEN
            -- If key exists in both, compare their 'speclty_key' values
            CASE
                WHEN mdm.speclty_key IS NOT DISTINCT FROM src.speclty_key THEN 'speclty_key: Match (Both: ' || COALESCE(mdm.speclty_key::text, 'NULL') || ')'
                ELSE 'speclty_key: Mismatch (MDM: ' || COALESCE(mdm.speclty_key::text, 'NULL') || ', SRC: ' || COALESCE(src.speclty_key::text, 'NULL') || ')'
            END
        WHEN mdm.prov_loc_speclty_key IS NOT NULL THEN 'speclty_key: Only in MDM (Value: ' || COALESCE(mdm.speclty_key::text, 'NULL') || ')'
        WHEN src.prov_loc_speclty_key IS NOT NULL THEN 'speclty_key: Only in SRC (Value: ' || COALESCE(src.speclty_key::text, 'NULL') || ')'
        ELSE 'speclty_key: N/A' -- Should not happen if common_prov_loc_speclty_key is not null
    END AS speclty_key_comparison,

    -- Comparison for 'speclty_cd'
    CASE
        WHEN mdm.prov_loc_speclty_key IS NOT NULL AND src.prov_loc_speclty_key IS NOT NULL THEN
            -- If key exists in both, compare their 'speclty_cd' values
            CASE
                WHEN mdm.speclty_cd IS NOT DISTINCT FROM src.speclty_cd THEN 'speclty_cd: Match (Both: ' || COALESCE(mdm.speclty_cd::text, 'NULL') || ')'
                ELSE 'speclty_cd: Mismatch (MDM: ' || COALESCE(mdm.speclty_cd::text, 'NULL') || ', SRC: ' || COALESCE(src.speclty_cd::text, 'NULL') || ')'
            END
        WHEN mdm.prov_loc_speclty_key IS NOT NULL THEN 'speclty_cd: Only in MDM (Value: ' || COALESCE(mdm.speclty_cd::text, 'NULL') || ')'
        WHEN src.prov_loc_speclty_key IS NOT NULL THEN 'speclty_cd: Only in SRC (Value: ' || COALESCE(src.speclty_cd::text, 'NULL') || ')'
        ELSE 'speclty_cd: N/A'
    END AS speclty_cd_comparison,

    -- Comparison for 'prim_speclty_ind'
    CASE
        WHEN mdm.prov_loc_speclty_key IS NOT NULL AND src.prov_loc_speclty_key IS NOT NULL THEN
            -- If key exists in both, compare their 'prim_speclty_ind' values
            CASE
                WHEN mdm.prim_speclty_ind IS NOT DISTINCT FROM src.prim_speclty_ind THEN 'prim_speclty_ind: Match (Both: ' || COALESCE(mdm.prim_speclty_ind::text, 'NULL') || ')'
                ELSE 'prim_speclty_ind: Mismatch (MDM: ' || COALESCE(mdm.prim_speclty_ind::text, 'NULL') || ', SRC: ' || COALESCE(src.prim_speclty_ind::text, 'NULL') || ')'
            END
        WHEN mdm.prov_loc_speclty_key IS NOT NULL THEN 'prim_speclty_ind: Only in MDM (Value: ' || COALESCE(mdm.prim_speclty_ind::text, 'NULL') || ')'
        WHEN src.prov_loc_speclty_key IS NOT NULL THEN 'prim_speclty_ind: Only in SRC (Value: ' || COALESCE(src.prim_speclty_ind::text, 'NULL') || ')'
        ELSE 'prim_speclty_ind: N/A'
    END AS prim_speclty_ind_comparison

FROM
    enac_nti_o.mdm_prov_loc_speclty mdm
FULL OUTER JOIN
    src ON mdm.prov_loc_speclty_key = src.prov_loc_speclty_key;





MASTER

  
SELECT
  src.prov_loc_speclty_key,
  src.speclty_key         AS src_speclty_key,
  mdm.speclty_key         AS mdm_speclty_key,
  src.speclty_cd          AS src_speclty_cd,
  mdm.speclty_cd          AS mdm_speclty_cd,
  src.prim_speclty_ind    AS src_prim_speclty_ind,
  mdm.prim_speclty_ind    AS mdm_prim_speclty_ind,

  CASE
    WHEN mdm.prov_loc_speclty_key IS NOT NULL THEN
      CASE
        WHEN src.speclty_key = mdm.speclty_key
          AND src.speclty_cd = mdm.speclty_cd
          AND src.prim_speclty_ind = mdm.prim_speclty_ind
        THEN 'Matched in Both'
        ELSE 'Key Matched, Data Mismatch'
      END
    ELSE 'Matched in SRC'
  END AS match_status

FROM
  src
LEFT JOIN enac_nti_o.mdm_prov_loc_speclty mdm
  ON src.prov_loc_speclty_key = mdm.prov_loc_speclty_key;


--
SELECT
  COALESCE(src.prov_loc_speclty_key, mdm.prov_loc_speclty_key) AS prov_loc_speclty_key,
  src.speclty_key         AS src_speclty_key,
  mdm.speclty_key         AS mdm_speclty_key,
  src.speclty_cd          AS src_speclty_cd,
  mdm.speclty_cd          AS mdm_speclty_cd,
  src.prim_speclty_ind    AS src_prim_speclty_ind,
  mdm.prim_speclty_ind    AS mdm_prim_speclty_ind,

  CASE
    WHEN src.prov_loc_speclty_key IS NOT NULL AND mdm.prov_loc_speclty_key IS NOT NULL THEN
      CASE
        WHEN src.speclty_key = mdm.speclty_key
          AND src.speclty_cd = mdm.speclty_cd
          AND src.prim_speclty_ind = mdm.prim_speclty_ind
        THEN 'Matched in Both'
        ELSE 'Key Matched, Data Mismatch'
      END
    WHEN src.prov_loc_speclty_key IS NOT NULL THEN 'Matched in SRC'
    WHEN mdm.prov_loc_speclty_key IS NOT NULL THEN 'Matched in mdm_prov_loc_speclty'
    ELSE 'Not exists in any one'  -- logically unreachab
/*88888*/


SELECT
  COALESCE(src.speclty_ckey, mdm.speclty_ckey) AS speclty_ckey,
  COALESCE(src.speclty_cd, mdm.speclty_cd) AS speclty_cd,
  COALESCE(src.prim_speclty_in, mdm.prim_speclty_in) AS prim_speclty_in,
  CASE
    WHEN src.speclty_ckey IS NOT NULL AND mdm.speclty_ckey IS NOT NULL THEN 'Matched in Both'
    WHEN src.speclty_ckey IS NOT NULL THEN 'Matched in SRC'
    WHEN mdm.speclty_ckey IS NOT NULL THEN 'Matched in mdm_prov_loc_speclty'
    ELSE 'Not exists in any one'
  END AS match_status
FROM
  src
FULL OUTER JOIN
  enac_nti_o.mdm_prov_loc_speclty mdm
  ON src.speclty_ckey = mdm.speclty_ckey
     AND src.speclty_cd = mdm.speclty_cd
     AND src.prim_speclty_in = mdm.prim_speclty_in;

-- The Query: Output if (A, B) is in TableA, TableB, or Both
SELECT
    COALESCE(ta.A, tb.A) AS A,
    COALESCE(ta.B, tb.B) AS B,
    CASE
        WHEN ta.A IS NOT NULL AND tb.A IS NOT NULL THEN 'In Both'
        WHEN ta.A IS NOT NULL THEN 'In TableA Only'
        WHEN tb.A IS NOT NULL THEN 'In TableB Only'
        ELSE 'Should not happen' -- This case would only occur if both A and B are NULL, which is unlikely for primary key components
    END AS presence_status
FROM
    (SELECT DISTINCT A, B FROM TableA) AS ta
FULL OUTER JOIN
    (SELECT DISTINCT A, B FROM TableB) AS tb
ON
    ta.A = tb.A AND ta.B = tb.B
ORDER BY A, B;


--Transaction WrapAround 

SELECT
    d.datname AS database_name,
    d.datfrozenxid AS last_vacuum_frozen_xid,
    txid_current() AS current_xid,
    (txid_current() + 1) AS next_transaction_id_will_be,
    (txid_current() - d.datfrozenxid) AS xids_since_last_freeze_point,
    s.setting::bigint AS autovacuum_freeze_max_age_limit,
    (s.setting::bigint - (txid_current() - d.datfrozenxid)) AS xids_remaining_until_autovac_threshold,
    ROUND(((txid_current() - d.datfrozenxid)::numeric * 100) / s.setting::numeric, 2) AS pct_of_autovac_freeze_threshold_used,
    2000000000 AS hard_wraparound_critical_limit_from_freeze, -- Approx. 2 Billion XIDs after datfrozenxid
    (2000000000 - (txid_current() - d.datfrozenxid)) AS xids_remaining_until_hard_limit,
    CASE
        WHEN (txid_current() - d.datfrozenxid) >= 1900000000 THEN 'CRITICAL: EXTREMELY CLOSE TO HARD WRAPAROUND LIMIT (~2 BILLION XIDs from datfrozenxid) - IMMEDIATE ACTION REQUIRED. DATABASE MAY SHUT DOWN!'
        WHEN (txid_current() - d.datfrozenxid) >= (s.setting::bigint * 0.95) THEN 'WARNING: Approaching autovacuum_freeze_max_age setting (95% used). Aggressive freezing expected soon!'
        WHEN (txid_current() - d.datfrozenxid) >= (s.setting::bigint * 0.80) THEN 'NOTICE: Approaching autovacuum_freeze_max_age (80% used). Monitor closely.'
        ELSE 'OK'
    END AS status_indicator
FROM pg_database d, pg_settings s
WHERE s.name = 'autovacuum_freeze_max_age'
  AND d.datname = current_database();

--02
SELECT
    d.datname AS database_name,
    pg_database_size(d.oid) AS database_size,
    pg_size_pretty(pg_database_size(d.oid)) AS database_size_pretty,
    current_setting('autovacuum_freeze_max_age')::bigint AS autovacuum_freeze_max_age,
    -- Current Transaction ID (next XID to be assigned)
    txid_current() AS current_xid_global,
    -- Database's oldest unfrozen XID
    d.datfrozenxid AS db_frozen_xid,
    -- Age of the database (transactions since oldest unfrozen XID)
    (txid_current() - d.datfrozenxid) AS db_age_xids,
    -- Gap to the 2 billion hard limit
    (2000000000 - (txid_current() - d.datfrozenxid)) AS gap_to_2billion_xids,
    -- Percentage left before hitting the 2 billion hard limit
    ROUND(((2000000000 - (txid_current() - d.datfrozenxid))::numeric / 2000000000) * 100, 2) AS percent_left_2billion,
    -- Percentage used relative to autovacuum_freeze_max_age threshold
    ROUND(((txid_current() - d.datfrozenxid)::numeric / current_setting('autovacuum_freeze_max_age')::bigint) * 100, 2) AS percent_of_autovacuum_threshold,

    -- Identifying Potential Blockers:
    -- Oldest running transaction (if any)
    (SELECT
        COALESCE(MIN(age(backend_xid)), 0)
     FROM pg_stat_activity
     WHERE backend_xid IS NOT NULL AND state <> 'idle'
       AND datname = d.datname
    ) AS oldest_running_xid_age,

    -- Oldest replication slot xmin (if any)
    (SELECT
        COALESCE(MIN(age(slot_xmin)), 0)
     FROM pg_replication_slots
     WHERE slot_xmin IS NOT NULL
       AND plugin IS DISTINCT FROM 'pgoutput' -- Exclude logical replication slots that don't hold back vacuum by default
    ) AS oldest_repl_slot_xid_age,

    -- Details of long-running transactions (if any)
    (SELECT
        jsonb_agg(jsonb_build_object(
            'pid', pid,
            'user', usename,
            'application_name', application_name,
            'client_addr', client_addr,
            'backend_start', backend_start,
            'query_start', query_start,
            'state', state,
            'query', query,
            'xid_age', age(backend_xid)
        ))
     FROM pg_stat_activity
     WHERE backend_xid IS NOT NULL AND state <> 'idle'
       AND datname = d.datname
       AND age(backend_xid) > 10000000 -- Adjust threshold for "long-running" as needed, e.g., 10M XIDs
    ) AS long_running_transactions_details,

    -- Details of unused replication slots (if any)
    (SELECT
        jsonb_agg(jsonb_build_object(
            'slot_name', slot_name,
            'plugin', plugin,
            'slot_type', slot_type,
            'active', active,
            'active_pid', active_pid,
            'restart_lsn', restart_lsn,
            'confirmed_flush_lsn', confirmed_flush_lsn,
            'wal_status', wal_status,
            'xid_age', age(slot_xmin)
        ))
     FROM pg_replication_slots
     WHERE slot_xmin IS NOT NULL AND active = FALSE
    ) AS inactive_replication_slots_details

FROM
    pg_database d
ORDER BY
    db_age_xids DESC;
/*
Explanation of the SQL Query:

database_name, database_size, database_size_pretty: Basic information about each database.
autovacuum_freeze_max_age: Shows the configured threshold for emergency vacuums.
current_xid_global: The current global transaction ID, which is the next ID that will be assigned. This is common across all databases in the cluster.
db_frozen_xid: The datfrozenxid for each specific database. This is the oldest transaction ID that is still visible within that database. All transactions older than this are considered "frozen" for MVCC purposes within that DB.
db_age_xids: This is the crucial metric: txid_current() - d.datfrozenxid. This tells you how "old" the oldest unfrozen transaction in that database is, in terms of XIDs. A higher number means it's closer to the wraparound danger.
gap_to_2billion_xids: This calculates how many more transaction IDs can be generated before the database hits the critical 2 billion XID threshold (where PostgreSQL would initiate a shutdown). A smaller number here means more urgent action is needed.
percent_left_2billion: The remaining percentage of the 2 billion XID limit before the hard shutdown. This gives you a clear indication of how much buffer you have.
percent_of_autovacuum_threshold: Shows how close your database's age is to the autovacuum_freeze_max_age setting. If this hits 100% (or, more commonly, 70-80% for a warning), autovacuum will become very aggressive.

*/
