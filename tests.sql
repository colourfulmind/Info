-- 1 (import)
CALL import_from_csv('peers', 'peers.csv', ',');
CALL import_from_csv('tasks', 'tasks.csv', ',');
CALL import_from_csv('checks', 'checks.csv', ',');
CALL import_from_csv('p2p', 'p2p.csv', ',');
CALL import_from_csv('friends', 'friends.csv', ',');
CALL import_from_csv('recommendations', 'recommendations.csv', ',');
CALL import_from_csv('time_tracking', 'time_tracking.csv', ',');
CALL import_from_csv('transferred_points', 'transferred_points.csv', ',');
CALL import_from_csv('Verter', 'verter.csv', ',');
CALL import_from_csv('XP', 'xp.csv', ',');

-- 1 (export)
CALL export_to_csv('peers', 'peers.csv', ',');
CALL export_to_csv('tasks', 'tasks.csv', ',');
CALL export_to_csv('checks', 'checks.csv', ',');
CALL export_to_csv('friends', 'friends.csv', ',');
CALL export_to_csv('recommendations', 'recommendations.csv', ',');
CALL export_to_csv('time_tracking', 'time_tracking.csv', ',');
CALL export_to_csv('transferred_points', 'transferred_points.csv', ',');
CALL export_to_csv('verter', 'verter.csv', ',');
CALL export_to_csv('xp', 'xp.csv', ',');

-- 2.1
CALL add_peer_to_peer_review('bernersl',
                             'stroustr',
                             'C2_SimpleBashUtils',
                             'Start',
                             '15:10:32');
CALL add_peer_to_peer_review('bernersl',
                             'stroustr',
                             'C2_SimpleBashUtils',
                             'Success',
                             '15:27:06');

CALL add_peer_to_peer_review('stroustr',
                             'gatesbil',
                             'C2_SimpleBashUtils',
                             'Start',
                             '17:46:07');

CALL add_peer_to_peer_review('stroustr',
                             'gatesbil',
                             'C2_SimpleBashUtils',
                             'Failure',
                             '18:11:54');

CALL add_peer_to_peer_review('gatesbil',
                             'thompson',
                             'C4_s21_Math',
                             'Start',
                             '18:16:46');

CALL add_peer_to_peer_review('gatesbil',
                             'thompson',
                             'C4_s21_Math',
                             'Success',
                             '18:35:58');

-- 2.2
CALL add_verter_review('bernersl',
                       'C2_SimpleBashUtils',
                       'Start',
                       '15:27:56');

CALL add_verter_review('bernersl',
                       'C2_SimpleBashUtils',
                       'Success',
                       '15:33:40');

-- Exception нет записи в таблице p2p
CALL add_verter_review('thompson',
                       'C4_s21_Math',
                       'Start',
                       '18:37:15');

CALL add_verter_review('gatesbil',
                       'C4_s21_Math',
                       'Start',
                       '18:37:15');

CALL add_verter_review('gatesbil',
                       'C4_s21_Math',
                       'Failure',
                       '13:39:06');

-- Exception повторное добавление
CALL add_verter_review('gatesbil',
                       'C4_s21_Math',
                       'Failure',
                       '13:39:06');

-- Exception нет записи о начале проверки
CALL add_peer_to_peer_review('bernersl',
                             'stroustr',
                             'CPP2_s21_Containers',
                             'Success',
                             '15:27:06');

CALL add_peer_to_peer_review('bernersl',
                             'stroustr',
                             'CPP2_s21_Containers',
                             'Start',
                             '15:10:32');

CALL add_peer_to_peer_review('bernersl',
                             'stroustr',
                             'CPP2_s21_Containers',
                             'Success',
                             '15:27:06');

-- 2.3
-- Срабатывает при вызове фунций из 2.1

-- 2.4
-- FAILED verter Failure
INSERT INTO xp(id, "check", xp_amount)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 8, 100);

-- FAILED task.xp < xp_amount
INSERT INTO xp(id, "check", xp_amount)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 2, 505);

-- FAILED p2p Failure
INSERT INTO xp(id, "check", xp_amount)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 3, 157);

-- SUCCESS
INSERT INTO xp(id, "check", xp_amount)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 2, 250);

INSERT INTO xp(id, "check", xp_amount)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 16, 653);

INSERT INTO xp(id, "check", xp_amount)
VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), (SELECT COALESCE(MAX(id), 0) FROM checks), 200);

-- 3.1
SELECT *
FROM get_transferred_points();

-- 3.2
SELECT *
FROM get_success_checks();

-- 3.3
SELECT *
FROM get_peer_tracking('21.01.2023');

-- 3.4
BEGIN;
CALL get_points_change();
FETCH ALL IN "cursor";
END;

-- 3.5
BEGIN;
CALL get_points_change_2();
FETCH ALL IN "cursor";
END;

-- 3.6
BEGIN;
CALL get_most_checked_task();
FETCH ALL IN "cursor";
END;

-- 3.7
BEGIN;
CALL get_completed_block();
FETCH ALL IN "cursor";
END;

-- 3.8
BEGIN;
CALL get_peer_recommendations();
FETCH ALL IN "cursor";
END;

-- 3.9
BEGIN;
CALL get_started_blocks('cursor', 'C', 'CPP');
FETCH ALL IN "cursor";
END;

-- 3.10
BEGIN;
CALL get_all_checks();
FETCH ALL IN "cursor";
END;

-- 3.11
BEGIN;
CALL get_completed_tasks('C2_SimpleBashUtils',
                         'C3_s21_StringPlus',
                         'C7_SmartCalc_v1.0',
                         'cursor');
FETCH ALL IN "cursor";
END;

-- 3.12
BEGIN;
CALL get_previous_tasks();
FETCH ALL IN "cursor";
END;

-- 3.13
BEGIN;
CALL get_successful_checks(2, 'cursor');
FETCH ALL IN "cursor";
END;

-- 3.14
BEGIN;
CALL get_peer_with_max_xp();
FETCH ALL IN "cursor";
END;

-- 3.15
BEGIN;
CALL get_entry_time('12:00:00', 3, 'cursor');
FETCH ALL IN "cursor";
END;

-- 3.16
BEGIN;
CALL get_exit_amount(300, 4, 'cursor');
FETCH ALL IN "cursor";
END;

-- 3.17
BEGIN;
CALL get_early_entry_percent();
FETCH ALL IN "cursor";
END;

-- 4.1
BEGIN;
CALL remove_tables_by_name('p2p');
END;

-- 4.2
BEGIN;
CALL get_names_and_parameters(total_amount := 0, function_parameters := '');
END;

-- 4.3
BEGIN;
CALL destroy_all_triggers(amount := 0);
END;

-- 4.4
BEGIN;
CALL get_names_and_description_by_parameter();
FETCH ALL IN "cursor";
END;
