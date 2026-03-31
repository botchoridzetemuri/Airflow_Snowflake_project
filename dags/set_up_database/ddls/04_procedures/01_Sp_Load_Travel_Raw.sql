CREATE OR REPLACE PROCEDURE AIRLINE_DWH.RAW.SP_LOAD_TRAVEL_RAW()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_loaded NUMBER;
    v_run_id STRING;
BEGIN
    v_run_id := UUID_STRING();

    TRUNCATE TABLE AIRLINE_DWH.RAW.TRAVEL_RAW;

    COPY INTO AIRLINE_DWH.RAW.TRAVEL_RAW
    FROM @AIRLINE_DWH.RAW.STG_AIRLINE
    FILE_FORMAT = (FORMAT_NAME = AIRLINE_DWH.RAW.FF_CSV)
    ON_ERROR = 'CONTINUE';

    v_rows_loaded := SQLROWCOUNT;

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
        'SP_LOAD_TRAVEL_RAW',
        'RAW.TRAVEL_RAW',
        'INSERT',
        :v_rows_loaded,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        NULL
    );

    COMMIT;

    RETURN 'RAW LOAD SUCCESS. Rows loaded: ' || v_rows_loaded;

EXCEPTION
    WHEN OTHER THEN
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
            'SP_LOAD_TRAVEL_RAW',
            'RAW.TRAVEL_RAW',
            'INSERT',
            NULL,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            'FAILED',
            SQLERRM
        );

        COMMIT;

        RETURN 'FAILED: ' || SQLERRM;
END;
$$;