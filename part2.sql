-- 1) Написать процедуру добавления P2P проверки
CREATE OR REPLACE PROCEDURE add_peer_to_peer_review(checked_peer VARCHAR,
                                                    new_checking_peer VARCHAR,
                                                    current_task_name VARCHAR,
                                                    state check_status,
                                                    peer_to_peer_time TIME)
AS
$add_peer_to_peer_review$
DECLARE
    check_id INTEGER := 0;
    p2p_id   INTEGER := 0;

BEGIN
    IF (state = 'Start') THEN
        IF ((SELECT p2p.state
             FROM p2p
                      JOIN checks ON checks.id = p2p."check"
             WHERE p2p.checking_peer = new_checking_peer
               AND checks.task = current_task_name
               AND p2p.id = (SELECT MAX(id) FROM p2p)) = 'Start')
        THEN
            RAISE EXCEPTION 'Добавление записи прервано ошибкой, у одного из пиров не завершена проверка!';
        ELSE
            check_id = (SELECT MAX(id) FROM checks) + 1;
            p2p_id = (SELECT MAX(id) FROM p2p) + 1;
            INSERT INTO checks
            VALUES (check_id, checked_peer, current_task_name, NOW());
            INSERT INTO p2p
            VALUES (p2p_id, check_id, new_checking_peer, state, peer_to_peer_time);
        END IF;
    ELSE
        IF ((SELECT checks.peer
             FROM checks
             WHERE id = (SELECT MAX(id) FROM checks)) = checked_peer
            AND (SELECT p2p.checking_peer
                 FROM p2p
                 WHERE id = (SELECT MAX(id) FROM p2p)) = new_checking_peer
            AND (SELECT checks.task
                 FROM checks
                 WHERE id = (SELECT MAX(id) FROM checks)) = current_task_name)
        THEN
            IF ((SELECT p2p.state
                 FROM p2p
                 WHERE p2p.id = (SELECT MAX(id) FROM p2p)) <> 'Start')
            THEN
                RAISE EXCEPTION 'Добавление записи прервано ошибкой, проверка уже завершена!';
            ELSE
                p2p_id = ((SELECT MAX(id) FROM p2p) + 1);
                INSERT INTO p2p
                VALUES (p2p_id,
                        (SELECT "check"
                         FROM p2p
                                  JOIN checks ON p2p."check" = checks.id
                         WHERE p2p.checking_peer = new_checking_peer
                           AND checks.task = current_task_name
                           AND checks.peer = checked_peer
                           AND p2p.id = (SELECT MAX(id) FROM p2p)
                           AND p2p.state = 'Start'),
                        new_checking_peer,
                        state,
                        peer_to_peer_time);
            END IF;
        ELSE
            RAISE EXCEPTION 'Добавление записи прервано ошибкой, завершите предыдущую проверку!';
        END IF;
    END IF;
END;
$add_peer_to_peer_review$
    LANGUAGE plpgsql;

-- 2) Написать процедуру добавления проверки Verter'ом
CREATE OR REPLACE PROCEDURE add_verter_review(nickname VARCHAR,
                                              task_name VARCHAR,
                                              verter_state check_status,
                                              check_time TIME)
AS
$add_verter_review$
DECLARE
    id_check INTEGER := (SELECT checks.id
                         FROM p2p
                                  INNER JOIN checks
                                             ON checks.id = p2p."check" AND p2p.state = 'Success'
                                                 AND checks.task = task_name
                                                 AND checks.peer = nickname
                         ORDER BY p2p."time"
                         LIMIT 1);
BEGIN
    if (id_check IS NOT NULL)
    THEN
        IF (verter_state = 'Start')
        THEN
            IF ((SELECT verter.state
                 FROM verter
                 WHERE verter.id = (SELECT MAX(id) FROM verter)) = 'Start')
            THEN
                RAISE EXCEPTION 'Добавление записи невозможно, проверка уже началась!';
            ELSE
                INSERT INTO verter
                VALUES ((SELECT MAX(id) + 1 FROM verter), id_check, verter_state, check_time);
            END IF;
        ELSE
            IF (id_check = (SELECT verter.check FROM verter WHERE id = (SELECT MAX(id) FROM verter)))
            THEN
                IF ((SELECT verter.state
                     FROM verter
                     WHERE verter.id = (SELECT MAX(id) FROM verter)) <> 'Start')
                THEN
                    RAISE EXCEPTION 'Добавление записи невозможно, проверка уже закончилась!';
                ELSE
                    INSERT INTO verter
                    VALUES ((SELECT MAX(id) FROM verter) + 1, id_check, verter_state, check_time);
                END IF;
            ELSE
                RAISE EXCEPTION 'Добавление записи прервано ошибкой, завершите предыдущую проверку!';
            END IF;
        END IF;
    ELSE
        RAISE EXCEPTION 'Данный проект не был проверен пиром';
    END IF;
END
$add_verter_review$ LANGUAGE plpgsql;

-- 3) Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints
CREATE OR REPLACE FUNCTION update_p2p_fnc()
    RETURNS TRIGGER AS
$update_p2p_trg$
DECLARE
    cte_checking_peer VARCHAR := (SELECT checking_peer
                                  FROM p2p
                                  ORDER BY p2p.id DESC
                                  LIMIT 1);
    cte_checked_peer  VARCHAR := (SELECT peer
                                  FROM checks
                                  WHERE id = (SELECT "check" FROM p2p ORDER BY p2p.id DESC LIMIT 1));
BEGIN
    IF ((SELECT state FROM p2p ORDER BY p2p.id DESC LIMIT 1) = 'Start') THEN
        IF (SELECT id
            FROM transferred_points
            WHERE cte_checking_peer = checking_peer
              AND cte_checked_peer = checked_peer) IS NOT NULL
        THEN
            UPDATE transferred_points
            SET points_amount = points_amount + 1
            WHERE cte_checking_peer = checking_peer
              AND cte_checked_peer = checked_peer;
        ELSE
            INSERT INTO transferred_points (id, checking_peer, checked_peer, points_amount)
            SELECT (SELECT MAX(id) + 1 FROM transferred_points), cte_checking_peer, cte_checked_peer, 1
            FROM transferred_points
            LIMIT 1;
            RETURN NEW;
        END IF;
    END IF;
    RETURN NULL;
END;
$update_p2p_trg$ LANGUAGE plpgsql;

CREATE TRIGGER update_p2p_trg
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION update_p2p_fnc();

-- 4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
CREATE OR REPLACE FUNCTION insert_xp_fnc()
    RETURNS TRIGGER AS
$insert_xp_trg$
DECLARE
    cte_p2p  VARCHAR := (SELECT state
                         FROM xp,
                              (SELECT DISTINCT p2p."check",
                                               checking_peer,
                                               CASE
                                                   WHEN v.state IS NULL THEN 'Success'
                                                   END AS state
                               FROM p2p
                                        LEFT JOIN verter AS v
                                                  ON v."check" = p2p."check"
                                                      AND v.state = 'Failure'
                                                      AND v.state <> 'Start'
                               WHERE p2p.state NOT IN ('Start', 'Failure')
                               ORDER BY 1) AS ct
                         WHERE NEW."check" = ct."check"
                         ORDER BY 1 DESC
                         LIMIT 1);
    cte_task INTEGER := (SELECT max_xp
                         FROM tasks,
                              xp,
                              checks
                         WHERE NEW."check" = checks.id
                           AND checks.task = tasks.title
                         ORDER BY xp.id DESC
                         LIMIT 1);
BEGIN
    IF (cte_p2p = 'Success' AND cte_task >= NEW.xp_amount) THEN
        RETURN NEW;
    ELSE
        DELETE FROM xp WHERE id = (SELECT id FROM xp ORDER BY xp.id DESC LIMIT 1);
    END IF;
    RETURN NULL;
END;
$insert_xp_trg$ LANGUAGE plpgsql;

CREATE TRIGGER insert_xp_trg
    AFTER INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION insert_xp_fnc();
END;
