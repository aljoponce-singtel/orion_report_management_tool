SELECT
    username AS user,
    DATE(last_login) AS last_login
FROM
    RestInterface_user
WHERE
    is_superuser = 1
ORDER BY
    username;