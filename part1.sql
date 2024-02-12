-- Напишите скрипт part1.sql, создающий базу данных и все таблицы, описанные выше.
CREATE DATABASE Info_v1;

CREATE TABLE IF NOT EXISTS peers
(
    nickname VARCHAR(255) PRIMARY KEY NOT NULL,
    birthday DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS tasks
(
    title       VARCHAR(255) PRIMARY KEY NOT NULL,
    parent_task TEXT,
    max_xp      BIGINT DEFAULT 0 CHECK ( max_xp >= 0 ),
    CONSTRAINT fk_tasks FOREIGN KEY (parent_task) REFERENCES tasks (title)
);

CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS checks
(
    id     BIGINT PRIMARY KEY NOT NULL,
    peer   VARCHAR(255)       NOT NULL,
    task   TEXT               NOT NULL,
    "date" DATE               NOT NULL,
    CONSTRAINT fk_checks_task FOREIGN KEY (task) REFERENCES tasks (title),
    CONSTRAINT fk_checks_peers FOREIGN KEY (peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS p2p
(
    id            BIGINT PRIMARY KEY NOT NULL,
    "check"       BIGINT             NOT NULL,
    checking_peer VARCHAR(255)       NOT NULL,
    state         check_status       NOT NULL,
    "time"        TIME DEFAULT CURRENT_TIME,
    CONSTRAINT fk_p2p_checks FOREIGN KEY ("check") REFERENCES checks (id),
    CONSTRAINT fk_p2p_peers FOREIGN KEY (checking_peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS verter
(
    id      BIGINT PRIMARY KEY NOT NULL,
    "check" BIGINT             NOT NULL,
    state   check_status       NOT NULL,
    "time"  TIME               NOT NULL,
    CONSTRAINT fk_verter_checks FOREIGN KEY ("check") REFERENCES checks (id)
);

CREATE TABLE IF NOT EXISTS transferred_points
(
    id            BIGINT PRIMARY KEY NOT NULL,
    checking_peer VARCHAR(255)       NOT NULL,
    checked_peer  VARCHAR(255)       NOT NULL,
    points_amount INTEGER DEFAULT 0 CHECK ( points_amount >= 0 ),
    CONSTRAINT fk_transferred_points_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers (nickname),
    CONSTRAINT fk_transferred_points_checked_peer FOREIGN KEY (checked_peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS friends
(
    id    BIGINT PRIMARY KEY NOT NULL,
    peer1 VARCHAR(255)       NOT NULL,
    peer2 VARCHAR(255)       NOT NULL,
    CONSTRAINT fk_friends_peer1 FOREIGN KEY (peer1) REFERENCES peers (nickname),
    CONSTRAINT fk_friends_peer2 FOREIGN KEY (peer2) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS recommendations
(
    id               BIGINT PRIMARY KEY NOT NULL,
    peer             VARCHAR(255)       NOT NULL,
    recommended_peer VARCHAR(255)       NOT NULL,
    CONSTRAINT fk_recommendations_peer FOREIGN KEY (peer) REFERENCES peers (nickname),
    CONSTRAINT fk_recommendations_recommended_peer FOREIGN KEY (recommended_peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS xp
(
    id        BIGINT PRIMARY KEY NOT NULL,
    "check"   BIGINT             NOT NULL,
    xp_amount BIGINT             NOT NULL,
    CONSTRAINT fk_xp_check FOREIGN KEY ("check") REFERENCES checks (id)
);

CREATE TABLE IF NOT EXISTS time_tracking
(
    id     BIGINT PRIMARY KEY NOT NULL,
    peer   VARCHAR(255)       NOT NULL,
    "date" DATE DEFAULT CURRENT_DATE,
    "time" TIME DEFAULT CURRENT_TIME,
    state  BIGINT             NOT NULL CHECK (state IN (1, 2)),
    CONSTRAINT fk_time_tracking_peers FOREIGN KEY (peer) REFERENCES peers (nickname)
);

-- Также внесите в скрипт процедуры, позволяющие импортировать и экспортировать данные
-- для каждой таблицы из файла/в файл с расширением .csv.
CREATE OR REPLACE PROCEDURE import_from_csv(table_name TEXT,
                                            file_name TEXT,
                                            separator CHAR(1) DEFAULT ',')
AS
$$
DECLARE
    -- Указать актуальный путь
    path_d TEXT := FORMAT('/Users/.../SQL2_Info21_v1.0-1/src/csv/%s', file_name);
BEGIN
    EXECUTE FORMAT('COPY %s FROM %L DELIMITERS %L CSV HEADER', table_name, path_d, separator);
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_to_csv(table_name TEXT,
                                          file_name TEXT,
                                          separator CHAR(1) DEFAULT ';')
AS
$$
DECLARE
    -- Указать актуальный путь
    path_d TEXT := FORMAT('/Users/.../SQL2_Info21_v1.0-1/src/csv/exp/%s', file_name);
BEGIN
    EXECUTE FORMAT('COPY %s TO %L DELIMITERS %L CSV HEADER', table_name, path_d, separator);
END;
$$
    LANGUAGE plpgsql;
