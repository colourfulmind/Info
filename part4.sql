-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных,
-- уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.
CREATE OR REPLACE PROCEDURE remove_tables_by_name(IN name_of_table VARCHAR)
AS
$remove_tables_by_name$
BEGIN
    FOR name_of_table IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE name_of_table || '%'
          AND table_schema = 'public'
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || name_of_table || ' CASCADE';
        END LOOP;
END;
$remove_tables_by_name$ LANGUAGE plpgsql;

-- 2) Создать хранимую процедуру с выходным параметром,
-- которая выводит список имен и параметров всех скалярных SQL функций пользователя в текущей базе данных.
-- Имена функций без параметров не выводить. Имена и список параметров должны выводиться в одну строку.
-- Выходной параметр возвращает количество найденных функций.
CREATE OR REPLACE PROCEDURE get_names_and_parameters(OUT total_amount INT,
                                                     OUT function_parameters VARCHAR)
AS
$get_names_and_parameters$
DECLARE
    fun_result     RECORD;
    func_par       VARCHAR = '';
    function_name  VARCHAR = '';
    parameter_list VARCHAR = '';
BEGIN
    total_amount = 0;
    FOR fun_result IN
        SELECT routines.routine_name, parameters.data_type
        FROM information_schema.routines
                 LEFT JOIN information_schema.parameters
                           ON routines.specific_name = parameters.specific_name
        WHERE routines.specific_schema NOT IN ('information_schema', 'pg_catalog')
          AND parameters.ordinal_position IS NOT NULL
        ORDER BY routines.routine_name, parameters.ordinal_position
        LOOP
            IF function_name != fun_result.routine_name
            THEN
                total_amount = total_amount + 1;
                function_name = fun_result.routine_name;
                parameter_list = fun_result.data_type;
                func_par := func_par || function_name || '(' || parameter_list || ')   ' || E'\n';
            END IF;
        END LOOP;
    function_parameters = func_par;
END;
$get_names_and_parameters$ LANGUAGE plpgsql;

-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных.
-- Выходной параметр возвращает количество уничтоженных триггеров.
CREATE OR REPLACE PROCEDURE destroy_all_triggers(OUT amount INTEGER) AS
$$
DECLARE
    name       TEXT;
    table_name TEXT;
BEGIN
    amount := 0;
    FOR name, table_name IN
        (SELECT trigger_name,
                event_object_table
         FROM information_schema.triggers
         WHERE trigger_schema = 'public')
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || name || ' ON ' || table_name || ';';
            amount = amount + 1;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 4) Создать хранимую процедуру с входным параметром,
-- которая выводит имена и описания типа объектов (только хранимых процедур и скалярных функций),
-- в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.
CREATE OR REPLACE PROCEDURE get_names_and_description_by_parameter(IN cursor REFCURSOR DEFAULT 'cursor',
                                                                   IN parameter VARCHAR DEFAULT 'xp') AS
$$
BEGIN
    OPEN cursor FOR
        SELECT routine_name AS names,
               routine_type AS descriptions
        FROM information_schema.routines
        WHERE routine_type IN ('FUNCTION', 'PROCEDURE')
          AND routine_schema = 'public'
          AND external_language SIMILAR TO ('%' || 'SQL' || '%')
          AND routine_definition SIMILAR TO ('%' || parameter || '%')
        ORDER BY names;
END;
$$ LANGUAGE plpgsql;
