CREATE OR REPLACE PROCEDURE AIRLINE_DWH.DWH.SP_LOAD_FACT_FROM_STREAM()
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

    INSERT INTO AIRLINE_DWH.DWH.FACT_TRAVEL (
        ROW_ID,
        PASSENGER_KEY,
        AIRPORT_KEY,
        ARRIVAL_AIRPORT_KEY,
        PILOT_KEY,
        TICKET_TYPE_KEY,
        STATUS_KEY,
        DEPARTURE_DATE_KEY
    )
    SELECT
        c.ROW_ID,
        p.PASSENGER_KEY,
        a.AIRPORT_KEY,
        aa.ARRIVAL_AIRPORT_KEY,
        pi.PILOT_KEY,
        tt.TICKET_TYPE_KEY,
        s.STATUS_KEY,
        d.DATE_KEY
    FROM AIRLINE_DWH.CLEAN.TRAVEL_CLEAN_STREAM c
    JOIN AIRLINE_DWH.DWH.DIM_PASSENGER p
        ON p.PASSENGER_ID = c.PASSENGER_ID
    JOIN AIRLINE_DWH.DWH.DIM_AIRPORT a
        ON a.AIRPORT_NAME = c.AIRPORT_NAME
       AND a.AIRPORT_COUNTRY_CODE = c.AIRPORT_COUNTRY_CODE
    JOIN AIRLINE_DWH.DWH.DIM_ARRIVAL_AIRPORT aa
        ON aa.ARRIVAL_AIRPORT = c.ARRIVAL_AIRPORT
    JOIN AIRLINE_DWH.DWH.DIM_PILOT pi
        ON pi.PILOT_NAME = c.PILOT_NAME
    JOIN AIRLINE_DWH.DWH.DIM_TICKET_TYPE tt
        ON tt.TICKET_TYPE = c.TICKET_TYPE
    JOIN AIRLINE_DWH.DWH.DIM_STATUS s
        ON s.FLIGHT_STATUS = c.FLIGHT_STATUS
       AND s.PASSENGER_STATUS = c.PASSENGER_STATUS
    LEFT JOIN AIRLINE_DWH.DWH.DIM_DATE d
        ON d.FULL_DATE = c.DEPARTURE_DATE
    WHERE c.METADATA$ACTION = 'INSERT'
      AND NOT EXISTS (
          SELECT 1
          FROM AIRLINE_DWH.DWH.FACT_TRAVEL f
          WHERE f.ROW_ID = c.ROW_ID
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
        'SP_LOAD_FACT_FROM_STREAM',
        'DWH.FACT_TRAVEL',
        'INSERT',
        :v_rows_inserted,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        NULL
    );

    COMMIT;

    RETURN 'SUCCESS. Inserted rows: ' || v_rows_inserted;

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
            'SP_LOAD_FACT_FROM_STREAM',
            'DWH.FACT_TRAVEL',
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