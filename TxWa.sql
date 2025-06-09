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

