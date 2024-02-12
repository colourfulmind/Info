-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
CREATE OR REPLACE FUNCTION get_transferred_points()
    RETURNS TABLE
            (
                "Peer1"        VARCHAR,
                "Peer2"        VARCHAR,
                "PointsAmount" INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH mutual_changes AS (SELECT tp1.id,
                                       tp1.checking_peer,
                                       tp1.checked_peer,
                                       tp1.points_amount
                                FROM transferred_points tp1
                                         JOIN transferred_points tp2
                                              ON tp1.checking_peer = tp2.checked_peer
                                                  AND tp1.checked_peer = tp2.checking_peer
                                GROUP BY tp1.id),
             unique_changes AS (SELECT checking_peer,
                                       checked_peer,
                                       points_amount
                                FROM transferred_points
                                EXCEPT ALL
                                SELECT checking_peer,
                                       checked_peer,
                                       points_amount
                                FROM mutual_changes),
             result AS (SELECT checking_peer,
                               checked_peer,
                               SUM(points_amount) AS points_amount
                        FROM unique_changes
                        GROUP BY checking_peer, checked_peer
                        UNION ALL
                        SELECT mutual1.checking_peer,
                               mutual1.checked_peer,
                               points1 - points2 AS points_amount
                        FROM (SELECT checking_peer,
                                     checked_peer,
                                     SUM(points_amount) points1
                              FROM mutual_changes
                              GROUP BY checking_peer, checked_peer) mutual1
                                 JOIN
                             (SELECT checking_peer,
                                     checked_peer,
                                     SUM(points_amount) points2
                              FROM mutual_changes
                              GROUP BY checking_peer, checked_peer) mutual2
                             ON mutual1.checking_peer = mutual2.checked_peer
                                 AND mutual1.checked_peer = mutual2.checking_peer
                        WHERE mutual1.checking_peer < mutual1.checked_peer)
        SELECT checking_peer          AS "Peer1",
               checked_peer           AS "Peer2",
               points_amount::INTEGER AS "PointsAmount"
        FROM result
        ORDER BY "Peer1", "Peer2";
END;
$$
    LANGUAGE plpgsql;

-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
CREATE OR REPLACE FUNCTION get_success_checks()
    RETURNS TABLE
            (
                "Peer" VARCHAR,
                "Task" TEXT,
                "XP"   BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH cte_success AS (SELECT DISTINCT p2p."check",
                                             checking_peer,
                                             CASE
                                                 WHEN v.state IS NULL THEN 'Success'
                                                 END AS state
                             FROM p2p
                                      LEFT JOIN verter AS v
                                                ON v."check" = p2p."check"
                                                    AND v.state = 'Failure'
                                                    AND v.state <> 'Start'
                             WHERE p2p.state <> 'Start'
                               AND p2p.state <> 'Failure'
                             ORDER BY 1)
        SELECT checks.peer                     AS "Peer",
               SPLIT_PART(checks.task, '_', 1) AS "Task",
               xp_amount                       AS "XP"
        FROM xp,
             checks,
             cte_success AS ct
        WHERE ct."check" = checks.id
          AND xp."check" = checks.id
        ORDER BY 1, 2;
END;
$$
    LANGUAGE plpgsql;

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
CREATE OR REPLACE FUNCTION get_peer_tracking(target_date VARCHAR DEFAULT '21.01.2023')
    RETURNS TABLE
            (
                Peer VARCHAR
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH T1 AS (SELECT tt1.peer, state
                    FROM time_tracking AS tt1
                    WHERE TO_DATE(target_date, 'DD.MM.YYYY') = tt1.date
                      AND state = 1),
             T2 AS (SELECT tt2.peer, state
                    FROM time_tracking AS tt2
                    WHERE TO_DATE(target_date, 'DD.MM.YYYY') = tt2.date
                      AND state = 2)
        SELECT T1.peer AS "Peer"
        FROM T1
                 LEFT JOIN T2
                           ON T2.peer = T1.peer
        WHERE T2.peer IS NULL
        ORDER BY 1;
END;
$$
    LANGUAGE plpgsql;

-- 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
CREATE OR REPLACE PROCEDURE get_points_change(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH incomes AS (SELECT checking_peer,
                                SUM(points_amount) AS income_points
                         FROM transferred_points
                         GROUP BY checking_peer),
             outcomes AS (SELECT checked_peer,
                                 SUM(points_amount) AS outcome_points
                          FROM transferred_points
                          GROUP BY checked_peer),
             middle_result AS (SELECT *
                               FROM incomes
                                        FULL JOIN outcomes
                                                  ON incomes.checking_peer = outcomes.checked_peer),
             result AS (SELECT COALESCE(checking_peer, checked_peer)                    AS peer_nickname,
                               COALESCE(income_points, 0) - COALESCE(outcome_points, 0) AS points
                        FROM middle_result)

        SELECT peer_nickname   AS "Peer",
               points::INTEGER AS "PointsChange"
        FROM result
        ORDER BY "PointsChange" DESC, "Peer";
END;
$$
    LANGUAGE plpgsql;

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
CREATE OR REPLACE PROCEDURE get_points_change_2(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH incomes AS (SELECT "Peer1",
                                SUM("PointsAmount") AS income_points
                         FROM get_transferred_points()
                         GROUP BY "Peer1"),
             outcomes AS (SELECT "Peer2",
                                 SUM("PointsAmount") AS outcome_points
                          FROM get_transferred_points()
                          GROUP BY "Peer2"),
             middle_result AS (SELECT *
                               FROM incomes
                                        FULL JOIN outcomes
                                                  ON incomes."Peer1" = outcomes."Peer2"),
             result AS (SELECT COALESCE("Peer1", "Peer2")                               AS peer_nickname,
                               COALESCE(income_points, 0) - COALESCE(outcome_points, 0) AS points
                        FROM middle_result)
        SELECT peer_nickname   AS "Peer",
               points::INTEGER AS "PointsChange"
        FROM result
        ORDER BY "PointsChange" DESC, "Peer";
END;
$$
    LANGUAGE plpgsql;

-- 6) Определить самое часто проверяемое задание за каждый день
CREATE OR REPLACE PROCEDURE get_most_checked_task(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH group_q AS (SELECT date, task, COUNT(*)
                         FROM checks
                         GROUP BY 1, 2
                         ORDER BY 1, 3 DESC)
        SELECT Q1.date                     AS "Day",
               SPLIT_PART(Q1.task, '_', 1) AS "Task"
        FROM group_q AS Q1
        WHERE Q1.count = (SELECT MAX(Q2.count)
                          FROM group_q AS Q2
                          WHERE Q1.date = Q2.date)
        ORDER BY 1, 2;
END;
$$
    LANGUAGE plpgsql;

-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
CREATE OR REPLACE PROCEDURE get_completed_block(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH p2p_checks AS (SELECT peer AS checked_peer, "date"
                            FROM checks
                                     JOIN p2p
                                          ON checks.id = p2p."check"
                            WHERE (SPLIT_PART(task, '_', 1) = 'C8'
                                OR SPLIT_PART(task, '_', 1) = 'DO6'
                                OR SPLIT_PART(task, '_', 1) = 'CPP9'
                                OR SPLIT_PART(task, '_', 1) = 'A8'
                                OR SPLIT_PART(task, '_', 1) = 'SQL3')
                              AND p2p.state = 'Success')
        SELECT checked_peer AS "Peer",
               "date"       AS "Day"
        FROM p2p_checks;
END;
$$
    LANGUAGE plpgsql;

-- 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
CREATE OR REPLACE PROCEDURE get_peer_recommendations(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH all_peers AS (SELECT peer1, peer2
                           FROM friends
                           UNION
                           SELECT peer2, peer1
                           FROM friends),
             recommendations AS (SELECT peer1, recommended_peer, COUNT(*) as total
                                 FROM all_peers
                                          JOIN recommendations ON all_peers.peer2 = recommendations.peer
                                 WHERE peer1 != recommended_peer
                                 GROUP BY peer1, recommended_peer)
        SELECT DISTINCT recommendation1.peer1                 AS "Peer",
                        MAX(recommendation1.recommended_peer) AS "RecommendedPeer"
        FROM recommendations recommendation1
        WHERE recommendation1.total = (SELECT MAX(total)
                                       FROM recommendations recommendation2
                                       WHERE recommendation1.peer1 = recommendation2.peer1)
        GROUP BY peer1;
END;
$$
    LANGUAGE plpgsql;

-- 9) Определить процент пиров, которые: приступили только к блоку 1, приступили только к блоку 2, приступили к обоим, не приступили ни к одному
CREATE OR REPLACE PROCEDURE get_started_blocks(IN cursor REFCURSOR DEFAULT 'cursor',
                                               IN block1 VARCHAR DEFAULT 'C',
                                               IN block2 VARCHAR DEFAULT 'CPP') AS
$$
BEGIN
    OPEN cursor FOR
        WITH first_block AS (SELECT nickname
                             FROM peers
                                      JOIN checks
                                           ON peers.nickname = checks.peer
                             WHERE task SIMILAR TO (block1 || '[0-9]%')
                             GROUP BY nickname),
             second_block AS (SELECT nickname
                              FROM peers
                                       JOIN checks
                                            ON peers.nickname = checks.peer
                              WHERE task SIMILAR TO (block2 || '[0-9]%')
                              GROUP BY nickname),
             first_only AS (SELECT *
                            FROM first_block
                            EXCEPT
                            SELECT *
                            FROM second_block),
             second_only AS (SELECT *
                             FROM second_block
                             EXCEPT
                             SELECT *
                             FROM first_block),
             none_of_blocks AS ((SELECT nickname
                                 FROM peers)
                                EXCEPT
                                (SELECT *
                                 FROM first_block
                                 UNION
                                 SELECT *
                                 FROM second_block)),
             both_of_blocks AS (SELECT *
                                FROM first_block
                                INTERSECT
                                SELECT *
                                FROM second_block)
        SELECT CASE
                   WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0.0
                   ELSE ROUND((SELECT COUNT(*) FROM first_only)::NUMERIC /
                              (SELECT COUNT(*) FROM peers) * 100, 2)
                   END AS "StartedBlock1",
               CASE
                   WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0.0
                   ELSE ROUND((SELECT COUNT(*) FROM second_only)::NUMERIC /
                              (SELECT COUNT(*) FROM peers) * 100, 2)
                   END AS "StartedBlock2",
               CASE
                   WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0.0
                   ELSE ROUND((SELECT COUNT(*) FROM both_of_blocks)::NUMERIC /
                              (SELECT COUNT(*) FROM peers) * 100, 2)
                   END AS "StartedBothBlocks",
               CASE
                   WHEN (SELECT COUNT(*) FROM peers) = 0 THEN 0.0
                   ELSE ROUND((SELECT COUNT(*) FROM none_of_blocks)::NUMERIC /
                              (SELECT COUNT(*) FROM peers) * 100, 2)
                   END AS "DidntStartAnyBlock";
END;
$$ LANGUAGE plpgsql;

-- 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
CREATE OR REPLACE PROCEDURE get_all_checks(IN cursor REFCURSOR DEFAULT 'cursor') AS
$$
BEGIN
    OPEN cursor FOR
        WITH match_date AS (SELECT checks.id,
                                   peers.birthday,
                                   checks."date"
                            FROM peers
                                     JOIN checks
                                          ON SPLIT_PART(peers.birthday::TEXT, '-', 3) =
                                             SPLIT_PART(checks."date"::TEXT, '-', 3)
                                              AND SPLIT_PART(peers.birthday::TEXT, '-', 2) =
                                                  SPLIT_PART(checks."date"::TEXT, '-', 2)),
             all_states AS (SELECT state
                            FROM p2p
                                     JOIN match_date ON match_date.id = p2p."check"
                            WHERE state IN ('Success', 'Failure'))
        SELECT CASE
                   WHEN (SELECT COUNT(*) FROM all_states) = 0 THEN 0.0
                   ELSE
                       ROUND((SELECT COUNT(*)
                              FROM all_states
                              WHERE state = 'Success')::NUMERIC /
                             (SELECT COUNT(*) FROM all_states) * 100, 2)
                   END AS "SuccessfulChecks",
               CASE
                   WHEN (SELECT COUNT(*) FROM all_states) = 0 THEN 0.0
                   ELSE
                       ROUND((SELECT COUNT(*)
                              FROM all_states
                              WHERE state = 'Failure')::NUMERIC /
                             (SELECT COUNT(*) FROM all_states) * 100, 2)
                   END AS "UnsuccessfulChecks";
END;
$$
    LANGUAGE plpgsql;

-- 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
CREATE OR REPLACE PROCEDURE get_completed_tasks(task_1 TEXT DEFAULT 'C2_SimpleBashUtils',
                                                task_2 TEXT DEFAULT 'C3_s21_StringPlus',
                                                task_3 TEXT DEFAULT 'C7_SmartCalc_v1.0',
                                                IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH cte_ss AS (SELECT DISTINCT p2p."check",
                                        checking_peer,
                                        CASE
                                            WHEN v.state IS NULL THEN 'Success'
                                            END AS state
                        FROM p2p
                                 LEFT JOIN verter AS v
                                           ON v."check" = p2p."check"
                                               AND v.state = 'Failure'
                                               AND v.state <> 'Start'
                        WHERE p2p.state <> 'Start'
                          AND p2p.state <> 'Failure'
                        ORDER BY 1),
             task_1 AS (SELECT peer
                        FROM checks,
                             cte_ss
                        WHERE cte_ss."check" = checks.id
                          AND task = $1),
             task_2 AS (SELECT peer
                        FROM checks,
                             cte_ss
                        WHERE cte_ss."check" = checks.id
                          AND task = $2),
             task_3 AS (SELECT peer
                        FROM checks,
                             cte_ss
                        WHERE cte_ss."check" = checks.id
                          AND task = $3),
             task_ss AS ((SELECT peer FROM task_1)
                         INTERSECT
                         (SELECT peer FROM task_2))
        SELECT peer
        FROM task_ss
        EXCEPT
        SELECT peer
        FROM task_3;
END;
$$
    LANGUAGE plpgsql;

-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
CREATE OR REPLACE PROCEDURE get_previous_tasks(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH RECURSIVE pr_task_prevCount (i, title, parent_task) AS (SELECT 0 AS i,
                                                                            title,
                                                                            parent_task
                                                                     FROM tasks
                                                                     WHERE title = 'C2_SimpleBashUtils'
                                                                     UNION ALL
                                                                     SELECT i + 1 AS i,
                                                                            t.title,
                                                                            t.parent_task
                                                                     FROM tasks t
                                                                              JOIN pr_task_prevCount r
                                                                                   ON t.parent_task = r.title)
        SELECT SPLIT_PART(title::TEXT, '_', 1) AS "Task",
               i                               AS "PrevCount"
        FROM pr_task_prevCount
        ORDER BY 2 DESC;
END;
$$
    LANGUAGE plpgsql;

-- 13) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
CREATE OR REPLACE PROCEDURE get_successful_checks(IN N INTEGER DEFAULT 2,
                                                  IN cursor REFCURSOR DEFAULT 'cursor') AS
$$
BEGIN
    OPEN cursor FOR
        WITH succesful_checks AS (SELECT checks."date"
                                  FROM checks
                                           JOIN p2p ON checks.id = p2p."check"
                                           LEFT JOIN verter ON checks.id = verter."check"
                                           JOIN tasks ON checks.task = tasks.title
                                           JOIN xp ON checks.id = xp."check"
                                  WHERE p2p.state = 'Success'
                                    AND (verter.state = 'Success' OR verter.state IS NULL)
                                    AND xp.xp_amount >= tasks.max_xp * 0.8)
        SELECT SPLIT_PART("date"::TEXT, '-', 3) || '.' ||
               SPLIT_PART("date"::TEXT, '-', 2) || '.' ||
               SPLIT_PART("date"::TEXT, '-', 1) AS "Date"
        FROM succesful_checks
        GROUP BY "date"
        HAVING COUNT("date") >= N;
END;
$$ LANGUAGE plpgsql;

-- 14) Определить пира с наибольшим количеством XP
CREATE OR REPLACE PROCEDURE get_peer_with_max_xp(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH tb_sum_xp AS (SELECT peer, SUM(xp_amount) AS "XP"
                           FROM xp,
                                checks
                           WHERE xp."check" = checks.id
                           GROUP BY 1
                           ORDER BY 2 DESC)
        SELECT peer AS "Peer",
               "XP"
        FROM tb_sum_xp
        LIMIT 1;
END;
$$
    LANGUAGE plpgsql;

-- 15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
CREATE OR REPLACE PROCEDURE get_entry_time(time_p TIME DEFAULT '12:00:00',
                                           N BIGINT DEFAULT 3,
                                           IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH tb_peer AS (SELECT peer, COUNT(*) AS amount
                         FROM time_tracking
                         WHERE state = 1
                           AND time < time_p
                         GROUP BY 1)
        SELECT peer   AS "Peer",
               amount AS "Amount"
        FROM tb_peer
        WHERE amount > N;
END;
$$
    LANGUAGE plpgsql;

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз
CREATE OR REPLACE PROCEDURE get_exit_amount(N BIGINT DEFAULT 300,
                                            M BIGINT DEFAULT 4,
                                            IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH tb_peer AS (SELECT peer, COUNT(*) AS amount
                         FROM time_tracking
                         WHERE state = 2
                           AND date > NOW()::DATE - N::INTEGER
                         GROUP BY 1)
        SELECT peer   AS "Peer",
               amount AS "Amount"
        FROM tb_peer
        WHERE amount > M;
END;
$$
    LANGUAGE plpgsql;

-- 17) Определить для каждого месяца процент ранних входов
CREATE OR REPLACE PROCEDURE get_early_entry_percent(IN cursor REFCURSOR DEFAULT 'cursor')
AS
$$
BEGIN
    OPEN cursor FOR
        WITH general_mons AS (SELECT mons::DATE AS g_mons
                              FROM GENERATE_SERIES('2023-01-01'::DATE, '2023-12-31'::DATE, '1 mons') AS mons
                              ORDER BY g_mons),
             total_visit AS (SELECT g_mons, COUNT(*) AS total
                             FROM time_tracking AS tt,
                                  general_mons,
                                  peers
                             WHERE SPLIT_PART(g_mons::TEXT, '-', 2) = SPLIT_PART(birthday::TEXT, '-', 2)
                               AND peers.nickname = tt.peer
                               AND tt.state = 1
                             GROUP BY 1
                             ORDER BY 1),
             early_visit AS (SELECT g_mons, COUNT(*) AS early
                             FROM time_tracking AS tt,
                                  general_mons,
                                  peers
                             WHERE SPLIT_PART(g_mons::TEXT, '-', 2) = SPLIT_PART(birthday::TEXT, '-', 2)
                               AND peers.nickname = tt.peer
                               AND tt.state = 1
                               AND tt.time < '12:00:00'::TIME
                             GROUP BY 1
                             ORDER BY 1)
        SELECT TO_CHAR(gm.g_mons, 'Month') AS "Month",
               CASE
                   WHEN early IS NULL OR total IS NULL THEN '0'
                   ELSE ROUND((early::NUMERIC / total::NUMERIC * 100), 2)
                   END                     AS "EarlyEntries"
        FROM general_mons AS gm
                 LEFT JOIN early_visit AS e
                           ON gm.g_mons = e.g_mons
                 LEFT JOIN total_visit AS t
                           ON e.g_mons = t.g_mons
        ORDER BY gm.g_mons;
END;
$$
    LANGUAGE plpgsql;
