SELECT
    DISTINCT PER.role AS 'group_id',
    PER.email AS 'user',
    PER.user_id,
    (
        CASE
            WHEN (
                PER.email LIKE '%singtel.com'
                AND NOT (
                    PER.email LIKE '%,%'
                    OR PER.email LIKE '%;%'
                    OR PER.email LIKE '%\%'
                    OR PER.email LIKE '%/%'
                ) -- AND PER.email NOT REGEXP '[,;\/\\\\]'
            ) THEN 'yes'
            ELSE 'no'
        END
    ) AS is_valid,
    (
        CASE
            WHEN GRP.is_enabled = 1 THEN 'yes'
            ELSE 'no'
        END
    ) AS is_enabled,
    USR_ACTUAL.username AS 'actual_user',
    USR_ACTUAL.id AS 'actual_user_id',
    USR_EXPECTED.username AS 'expected_user',
    USR_EXPECTED.id AS 'expected_user_id',
    PER.created_at,
    PER.updated_at
FROM
    RestInterface_person PER
    JOIN auto_escalation_escalationmatrix MTX ON MTX.person_id = PER.id
    JOIN auto_escalation_escalationgroup GRP ON GRP.person_id = PER.id
    LEFT JOIN RestInterface_user USR_ACTUAL ON USR_ACTUAL.id = PER.user_id
    LEFT JOIN RestInterface_user USR_EXPECTED ON USR_EXPECTED.username = TRIM(PER.email)
WHERE
    PER.email != ''
    AND (
        TRIM(PER.email) != USR_ACTUAL.username
        OR PER.user_id IS NULL
    )
ORDER BY
    PER.email,
    PER.role;