SELECT
    DISTINCT ESCGRP.department,
    ESCGRP.function,
    PER.role AS group_id,
    MTX.level,
    USR.username,
    ESCGRP.is_enabled
FROM
    RestInterface_person PER
    LEFT JOIN auto_escalation_escalationmatrix MTX ON MTX.person_id = PER.id
    LEFT JOIN RestInterface_user USR ON USR.id = MTX.user_id
    LEFT JOIN auto_escalation_escalationgroup ESCGRP ON ESCGRP.person_id = PER.id
WHERE
    USR.username = "alanlam@singtel.com"
ORDER BY
    ESCGRP.department,
    ESCGRP.function,
    PER.role;