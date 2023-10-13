UPDATE
    RestInterface_person PER
    JOIN auto_escalation_escalationmatrix MTX ON MTX.person_id = PER.id
    JOIN auto_escalation_escalationgroup GRP ON GRP.person_id = PER.id
    LEFT JOIN RestInterface_user USR_ACTUAL ON USR_ACTUAL.id = PER.user_id
    LEFT JOIN RestInterface_user USR_EXPECTED ON USR_EXPECTED.username = TRIM(PER.email)
SET
    PER.user_id = USR_EXPECTED.id
WHERE
    PER.email != ''
    AND (
        TRIM(PER.email) != USR_ACTUAL.username
        OR PER.user_id IS NULL
    )
    AND (
        PER.email LIKE '%singtel.com'
        AND NOT (
            PER.email LIKE '%,%'
            OR PER.email LIKE '%;%'
            OR PER.email LIKE '%\%'
            OR PER.email LIKE '%/%'
        )
    );