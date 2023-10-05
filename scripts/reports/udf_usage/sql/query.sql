SELECT
    DISTINCT DATE(created_at) AS login_date,
    DAYNAME(created_at) AS day_name,
    username
FROM
    RestInterface_authlog
WHERE
    NOT (
        username = 'admin'
        OR username = 'gspuser@singtel.com'
        OR username = 'projectmanager@singtel.com'
        OR username = 'executivemanager@singtel.com'
        OR username = 'productmanager@singtel.com'
        OR username = 'accountmanager@singtel.com'
        OR username = 'queueowner@singtel.com'
        OR username = 'opsmanager'
        OR username = 'mluser@singtel.com'
        OR username = 'aljo.ponce@singtel.com'
        OR username = 'jiangxu@ncs.com.sg'
        OR username = 'adelinethk@singtel.com'
        OR username = 'jacob.toh@singtel.com'
        OR username = 'yuchen.liu@singtel.com'
        OR username = 'weiwang.thang@singtel.com'
    )
    AND user_type = 'Account Manager'
    AND created_at BETWEEN '{}'
    AND '{}'
    AND status = 1
ORDER BY
    login_date,
    username;