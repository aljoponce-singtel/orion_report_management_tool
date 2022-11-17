SELECT
    DISTINCT USR.username
FROM
    RestInterface_user USR
WHERE
    NOT (
        USR.username = 'admin'
        OR USR.username = 'gspuser@singtel.com'
        OR USR.username = 'projectmanager@singtel.com'
        OR USR.username = 'executivemanager@singtel.com'
        OR USR.username = 'productmanager@singtel.com'
        OR USR.username = 'accountmanager@singtel.com'
        OR USR.username = 'queueowner@singtel.com'
        OR USR.username = 'opsmanager'
        OR USR.username = 'mluser@singtel.com'
        OR USR.username = 'aljo.ponce@singtel.com'
        OR USR.username = 'jiangxu@ncs.com.sg'
        OR USR.username = 'adelinethk@singtel.com'
        OR USR.username = 'jacob.toh@singtel.com'
        OR USR.username = 'yuchen.liu@singtel.com'
        OR USR.username = 'weiwang.thang@singtel.com'
    )
    AND USR.team = 'Account Manager'
    AND DATE(USR.last_login) BETWEEN '{}'
    AND '{}'
ORDER BY
    USR.username;