CREATE OR REPLACE PROCEDURE AIRLINE_DWH.CLEAN.SP_LOAD_TRAVEL_CLEAN()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted NUMBER;
    v_run_id STRING;
    v_error_message STRING;
BEGIN

    v_run_id := UUID_STRING();

    INSERT INTO AIRLINE_DWH.CLEAN.TRAVEL_CLEAN (
        ROW_ID,
        PASSENGER_ID,
        FIRST_NAME,
        LAST_NAME,
        GENDER,
        AGE,
        NATIONALITY,
        AIRPORT_NAME,
        AIRPORT_COUNTRY_CODE,
        COUNTRY_NAME,
        AIRPORT_CONTINENT,
        CONTINENTS,
        DEPARTURE_DATE,
        ARRIVAL_AIRPORT,
        PILOT_NAME,
        FLIGHT_STATUS,
        TICKET_TYPE,
        PASSENGER_STATUS
    )
    SELECT
        r."c1"  AS ROW_ID,
        r."c2"  AS PASSENGER_ID,
        r."c3"  AS FIRST_NAME,
        r."c4"  AS LAST_NAME,
        r."c5"  AS GENDER,
        r."c6"  AS AGE,
        r."c7"  AS NATIONALITY,
        r."c8"  AS AIRPORT_NAME,
        r."c9"  AS AIRPORT_COUNTRY_CODE,
        r."c10" AS COUNTRY_NAME,
        r."c11" AS AIRPORT_CONTINENT,
        r."c12" AS CONTINENTS,
        TRY_TO_DATE(r."c13") AS DEPARTURE_DATE,
        r."c14" AS ARRIVAL_AIRPORT,
        r."c15" AS PILOT_NAME,
        r."c16" AS FLIGHT_STATUS,
        r."c17" AS TICKET_TYPE,
        r."c18" AS PASSENGER_STATUS
    FROM AIRLINE_DWH.RAW.TRAVEL_RAW r
    WHERE NOT EXISTS (
        SELECT 1
        FROM AIRLINE_DWH.CLEAN.TRAVEL_CLEAN c
        WHERE c.ROW_ID = r."c1"
    );

    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO AIRLINE_DWH.AUDIT.ETL_LOG (
        RUN_ID,
        PROC_NAME,
        TARGET_TABLE,
        ACTION_TYPE,
        ROWS_AFFECTED,
        STARTED_AT,
        ENDED_AT,
        STATUS,
        ERROR_MESSAGE
    )
    VALUES (
        :v_run_id,
        'SP_LOAD_TRAVEL_CLEAN',
        'CLEAN.TRAVEL_CLEAN',
        'INSERT',
        :v_rows_inserted,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        NULL
    );

    COMMIT;

    RETURN 'CLEAN LOAD SUCCESS. Rows inserted: ' || v_rows_inserted;

EXCEPTION
    WHEN OTHER THEN
        v_error_message := SQLERRM;

        ROLLBACK;

        INSERT INTO AIRLINE_DWH.AUDIT.ETL_LOG (
            RUN_ID,
            PROC_NAME,
            TARGET_TABLE,
            ACTION_TYPE,
            ROWS_AFFECTED,
            STARTED_AT,
            ENDED_AT,
            STATUS,
            ERROR_MESSAGE
        )
        VALUES (
            COALESCE(:v_run_id, UUID_STRING()),
            'SP_LOAD_TRAVEL_CLEAN',
            'CLEAN.TRAVEL_CLEAN',
            'INSERT',
            NULL,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            'FAILED',
            :v_error_message
        );

        COMMIT;

        RETURN 'FAILED: ' || v_error_message;
END;
$$;