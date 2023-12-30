
--------------------------------------------------------
--  File created - piıtek-grudnia-22-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body GODW_CORE_LOADER
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "GODW_ADMIN"."GODW_CORE_LOADER" 
AS


  v_source_owner                   varchar2(30);
  v_source_table                   varchar2(30);
  v_source_table_long              varchar2(61);
  v_core_head_table                varchar2(30);
  v_core_vers_table                varchar2(30);
  v_core_vers_table_for_sequence   varchar2(30);
  v_core_head_table_long           varchar2(61);
  v_core_vers_table_long           varchar2(61);

  v_current_load_id                number;

  v_string                         varchar2(4000);
  v_counter                        number;

  v_source_name                    varchar2(30);
  v_core_owner                     varchar2(30);

  --v_core_curr_view                 varchar2(30);
  --v_core_curr_view_long            varchar2(61);





  CURSOR    c_tab_cols  (c_owner               VARCHAR2
                        ,c_table_name          VARCHAR2
                        ,c_column_selector     VARCHAR2)   IS
  SELECT    all_tab_columns.column_name
         ,  all_tab_columns.data_type
         ,  all_tab_columns.nullable
  FROM      all_tab_columns
  WHERE     all_tab_columns.table_name  = UPPER(c_table_name)
  AND       all_tab_columns.owner       = c_owner
  AND    (
            (c_column_selector    = 'ALL'  )
           OR
            (c_column_selector    = 'HEADER_SCD1'
              AND
             all_tab_columns.column_name NOT IN ('DWH_ID'
                                                ,'DWH_SOURCE_ID'
                                                ,'DWH_CR_LOAD_ID'
                                                ,'DWH_CR_JOB')
              AND
             all_tab_columns.column_name NOT LIKE 'DWH_SOURCE_KEY%'
            )
           OR
            (c_column_selector    = 'VERSION_SCD2'
              AND
             all_tab_columns.column_name NOT IN ('DWH_ID'
                                                ,'DWH_SOURCE_ID'
                                                ,'DWH_CR_LOAD_ID'
                                                ,'DWH_CR_JOB'
                                                ,'DWH_HEAD_ID'
                                                ,'DWH_VERS_ID'
                                                ,'DWH_VALID_FROM'
                                                ,'DWH_VALID_TO')
              AND
             all_tab_columns.column_name NOT LIKE 'DWH_SOURCE_KEY%'
            )
           OR
            (c_column_selector    = 'WITHOUT_DWH_COLS'
              AND
             all_tab_columns.column_name NOT IN ('DWH_ID'
                                                ,'DWH_SOURCE_ID'
                                                ,'DWH_CR_LOAD_ID'
                                                ,'DWH_UP_LOAD_ID'
                                                ,'DWH_CR_JOB'
                                                ,'DWH_UP_JOB'
                                                ,'DWH_HEAD_ID'
                                                ,'DWH_VERS_ID'
                                                ,'DWH_VALID_FROM'
                                                ,'DWH_VALID_TO')
              AND
             all_tab_columns.column_name NOT LIKE 'DWH_SOURCE_KEY%'
            )
         )
  ORDER BY  all_tab_columns.column_id;




  CURSOR    c_dwh_fk_columns    (c_table_name    VARCHAR2)     IS
  SELECT    all_tab_columns.column_name
  ,         all_tab_columns.nullable
  FROM      all_tab_columns
  WHERE     all_tab_columns.table_name  = UPPER(c_table_name)
  AND       all_tab_columns.column_name LIKE 'DWH%ID'
  AND       all_tab_columns.column_name NOT IN ('DWH_ID'
                                               ,'DWH_SOURCE_ID'
                                               ,'DWH_SOURCE_KEY'
                                               ,'DWH_CR_LOAD_ID'
                                               ,'DWH_UP_LOAD_ID'
                                               ,'DWH_CR_JOB'
                                               ,'DWH_UP_JOB'
                                               ,'DWH_HEAD_ID'
                                               ,'DWH_VERS_ID'
                                               ,'DWH_VALID_FROM'
                                               ,'DWH_VALID_TO')
  AND       all_tab_columns.owner       =  v_core_owner
  ORDER BY  all_tab_columns.column_id;


  CURSOR c_split_string     (c_string        VARCHAR2
                            ,c_delimiter     VARCHAR2) IS
  select     regexp_substr(c_string, '[^' || c_delimiter || ']+', 1, level) splitted_string
  from     dual
  connect by regexp_substr(c_string, '[^' || c_delimiter || ']+', 1, level) is not null;



  FUNCTION  get_column_exists  (i_owner          IN  VARCHAR2
                               ,i_table_name     IN  VARCHAR2
                               ,i_column_name    IN  VARCHAR2)  RETURN VARCHAR2  IS

    v_column_exists  VARCHAR2(1);

  BEGIN

    SELECT   'Y'
    INTO     v_column_exists
    FROM     all_tab_columns
    WHERE    all_tab_columns.owner       = i_owner
    AND      all_tab_columns.table_name  = i_table_name
    AND      all_tab_columns.column_name = i_column_name;

    RETURN v_column_exists;

  EXCEPTION
    WHEN OTHERS THEN
       RETURN 'N';

  END    get_column_exists;


  FUNCTION  get_escape_string (i_string    IN VARCHAR2)  RETURN VARCHAR2 IS

    v_string        VARCHAR2(32767);

  BEGIN

    v_string := i_string;

    v_string := REPLACE(v_string,'''','''''');


    RETURN v_string;



  END get_escape_string;


  PROCEDURE   update_work_progress  (i_log_id            IN       NUMBER
                                    ,i_number_of_rows    IN       NUMBER) IS  PRAGMA AUTONOMOUS_TRANSACTION;



  BEGIN


    UPDATE  godw_core_loader_log
    SET     godw_core_loader_log.rows_source_delta_detected = i_number_of_rows
    WHERE   godw_core_loader_log.log_id                     = i_log_id;

    commit;

  END update_work_progress;




  PROCEDURE   debug  (i_level      IN       NUMBER
                     ,i_message    IN       VARCHAR2
                     ,i_statement  IN       VARCHAR2 DEFAULT NULL) IS  PRAGMA AUTONOMOUS_TRANSACTION;



  BEGIN



    IF v_debug_level = 0 THEN
       GOTO end_log;
    END IF;

    IF i_level <= v_debug_level THEN

       insert into godw_core_loader_debug
                (load_id           ,line_no                           ,date_created ,description ,statement   ,ins_user ,ins_date ,upd_user ,upd_date ,ins_osuser                        ,upd_osuser                       )
         values (v_current_load_id ,sq_godw_core_loader_debug.nextval ,systimestamp ,i_message   ,i_statement ,user     ,sysdate  ,user     ,sysdate  ,sys_context('USERENV', 'OS_USER') ,sys_context('USERENV', 'OS_USER'));
       commit;
    END IF;


    <<end_log>>
    NULL;


  END debug;



  FUNCTION  get_seq_id      (i_table_name      IN  VARCHAR2)   RETURN NUMBER IS

    v_id             NUMBER;
    v_statement      VARCHAR2(4000);
  BEGIN

     v_statement := 'select ' || v_core_owner || '.sq_' || i_table_name || '.NEXTVAL from dual';
     debug(50,'statement in get_seq_id', v_statement);
     execute immediate v_statement INTO v_id ;
     debug(50,'new id = ' || v_id,NULL);

     RETURN  v_id;

  END get_seq_id;



   FUNCTION get_fk_replaced       (i_source_table_long           VARCHAR2
                                  ,i_core_owner                  VARCHAR2
                                  ,i_core_table                  VARCHAR2
                                  ,i_column_name                 VARCHAR2
                                 , i_column_nullable             VARCHAR2
                                 , i_fk_mapping                  VARCHAR2
                                 , i_source_table_rowid          ROWID
                                   )  RETURN  varchar2  IS

  v_fk_mapping                         VARCHAR2(4000);

  v_columns_origin                     VARCHAR2(4000);
  v_column_counter                     NUMBER := 0;
  v_pos                                NUMBER := 0;

  v_source_key_1                       VARCHAR2(50);
  v_source_key_2                       VARCHAR2(50);
  v_source_key_3                       VARCHAR2(50);
  v_source_key_4                       VARCHAR2(50);
  v_source_key_5                       VARCHAR2(50);
  v_statement                          VARCHAR2(4000);

  v_dwh_reference_table                VARCHAR2(30);

  v_id                                 NUMBER;



  BEGIN



   v_fk_mapping            := ',' || UPPER(REPLACE(i_fk_mapping,' ',''));


   debug(50,'fk: column= ' || i_core_table || '.' || i_column_name);

   -- get origin-column from source-table
   debug(50,'fk: v_fk_mapping= ' || upper(v_fk_mapping));

   FOR r in c_split_string (upper(v_fk_mapping),',') LOOP

     debug(50,'fk: r.splitted_string= '          || r.splitted_string);
     debug(50,'fk: v_fk_mapping i_column_name= ' || i_column_name);

     v_pos := INSTR(r.splitted_string,'=>' || i_column_name) ;
     debug(50,'fk: v_fk_mapping v_pos= '   || v_pos);

     IF v_pos > 0 then
       v_column_counter := v_column_counter + 1;
       v_string := SUBSTR(r.splitted_string,1,v_pos -1);

       IF  v_column_counter > 1 THEN
          v_columns_origin := v_columns_origin || ',';
       END IF;
       v_columns_origin := v_columns_origin || v_string;

       debug(50,'fk: v_columns_origin= ' || v_columns_origin);

     END IF;

   END LOOP;

   FOR i IN (v_column_counter + 1)..5 LOOP
     v_columns_origin := v_columns_origin || ',' || 'NULL';
   END LOOP;

   debug(50,'fk: columns_origin=' || v_columns_origin);



   -- now get the source-key of the source table

   v_statement := 'select '       ||         v_columns_origin
               || ' from '        ||         i_source_table_long
               || ' where rowid=' || '''' || i_source_table_rowid || '''';

   debug(50,'fk statement',v_statement);

--DBMS_OUTPUT.PUT_LINE(V_STATEMENT);
   execute immediate v_statement  into  v_source_key_1,v_source_key_2,v_source_key_3,v_source_key_4,v_source_key_5;


   if v_source_key_1 is null then
       debug(50,'source_key is null, so we set the singleton ' || to_char(c_singleton_id_unknown) ,NULL);

      IF i_column_nullable = 'Y' THEN  -- changed at 24-feb-2010
         return 'NULL';
      ELSE
        return  to_char(c_singleton_id_undefined);
      END IF;
   end if;

   debug(50,'fk v_source_key_1= ' || v_source_key_1,NULL);
   debug(50,'fk v_source_key_2= ' || v_source_key_2,NULL);
   debug(50,'fk v_source_key_3= ' || v_source_key_3,NULL);
   debug(50,'fk v_source_key_4= ' || v_source_key_4,NULL);
   debug(50,'fk v_source_key_5= ' || v_source_key_5,NULL);

   -- now get the core-reference -table


   debug(50,'fk parameters to get reference_table i_core_owner        = ' || i_core_owner,NULL);
   debug(50,'fk parameters to get reference_table UPPER(i_core_table) = ' || UPPER(i_core_table),NULL);
   debug(50,'fk parameters to get reference_table i_column_name       = ' || i_column_name,NULL);


   v_dwh_reference_table := godw_common.get_reference_table
                              (i_owner         =>       i_core_owner
                              ,i_table_name    => UPPER(i_core_table)
                              ,i_column_name   =>       i_column_name);


   debug(50,'fk parameters to get reference_table returned = '             || v_dwh_reference_table,NULL);

   debug(50,'fk v_dwh_reference_table= ' || v_dwh_reference_table,NULL);


   -- now get the primary-key of the core-reference table
   v_statement :=    'select dwh_id'
                ||   ' from ' || i_core_owner || '.' || v_dwh_reference_table;
   v_counter := 0;
   for r in (SELECT   all_tab_columns.column_name
             FROM     all_tab_columns
             WHERE    all_tab_columns.table_name =  v_dwh_reference_table
             AND      all_tab_columns.owner      =  i_core_owner
             AND      all_tab_columns.column_name LIKE ('DWH_SOURCE_KEY%')
             ORDER BY all_tab_columns.column_name) loop
     v_counter := v_counter + 1;
     if v_counter =  1 then
        v_statement := v_statement ||  ' where ';
     else
        v_statement := v_statement || ' and ';
     end if;
     if    v_counter = 1 then
        v_statement := v_statement || r.column_name || ' = '  || '''' || v_source_key_1 || '''';
     elsif v_counter = 2 then
        v_statement := v_statement || r.column_name || ' = '  || '''' || v_source_key_2 || '''';
     elsif v_counter = 3 then
        v_statement := v_statement || r.column_name || ' = '  || '''' || v_source_key_3 || '''';
     elsif v_counter = 4 then
        v_statement := v_statement || r.column_name || ' = '  || '''' || v_source_key_4 || '''';
     elsif v_counter = 5 then
        v_statement := v_statement || r.column_name || ' = '  || '''' || v_source_key_5 || '''';
     end if;

   end loop;


   debug(50,'fk statement for to get core-row= ',v_statement);

   begin
     execute immediate v_statement  into  v_id ;
   exception
     when no_data_found then
       v_id := c_singleton_id_unknown ;    -- use singleton value
   end;

   debug(50,'fk core-id is = ' || v_id,NULL);

   RETURN v_id;

  END get_fk_replaced;





  FUNCTION get_all_fks_replaced   (i_column_list_string          VARCHAR2
                                 , i_source_table_rowid          ROWID
                                 , i_core_table                  VARCHAR2
                                 , i_fk_mapping                  VARCHAR2
                                   )  RETURN  VARCHAR2   IS

  v_column_list_string                 VARCHAR2(4000);
  v_id_string                          VARCHAR2(100);

  BEGIN

    v_column_list_string    := i_column_list_string;
  --  v_fk_mapping            := ',' || UPPER(REPLACE(i_fk_mapping,' ',''));

    FOR r_dwh_fk_columns IN c_dwh_fk_columns (i_core_table) LOOP

        v_id_string  := get_fk_replaced (i_source_table_long  => v_source_table_long
                                        ,i_core_owner         => v_core_owner
                                        ,i_core_table         => i_core_table
                                        ,i_column_name        => r_dwh_fk_columns.column_name
                                        ,i_column_nullable    => r_dwh_fk_columns.nullable
                                        ,i_fk_mapping         => i_fk_mapping
                                        ,i_source_table_rowid => i_source_table_rowid
                                   )   ;

        debug(50,'fk core-id is = ' || v_id_string,NULL);
       v_column_list_string := REPLACE(v_column_list_string,'#' || r_dwh_fk_columns.column_name || '#'  ,v_id_string);

    END LOOP;

    RETURN  v_column_list_string;

  END get_all_fks_replaced;



  FUNCTION get_column_list_string   (i_owner              varchar2
                                    ,i_table_name         varchar2
                                    ,i_use_marker         varchar2
                                    ,i_column_selector    varchar2   ) RETURN  varchar2  IS

     v_string         VARCHAR2(4000);

  BEGIN

    FOR r_tab_cols IN c_tab_cols ( /* c_owner            */  i_owner
                                 , /* c_table_name       */  i_table_name
                                 , /* c_column_selector  */  i_column_selector)  LOOP

     IF v_string IS NOT  NULL   THEN
        v_string := v_string || ',';
     END IF;


     SELECT   v_string  || DECODE(i_use_marker,'Y','#','')  ||
                           r_tab_cols.column_name           ||
                           DECODE(i_use_marker,'Y','#','')
     INTO     v_string
     FROM     dual;


  END LOOP;


  RETURN v_string;


  END get_column_list_string;






  FUNCTION get_exists_in_core
                              (i_source_id                   NUMBER
                              ,i_source_table_long           VARCHAR2
                              ,i_core_head_table_long        VARCHAR2
                              ,i_core_vers_table_long        VARCHAR2
                              ,i_fk_mapping_head_table       VARCHAR2
                              ,i_fk_mapping_vers_table       VARCHAR2) RETURN VARCHAR2   IS

    v_statement                             VARCHAR2(32767);
    v_statement_function_call               VARCHAR2(32767);

  BEGIN

     -- prefix cleanse-table: source         (alias already defined in previous step)
     -- prefix core-head-table:  core_head
     -- prefix core-vers-table:  core_vers


   v_statement := ' not exists (select 1 from ' || i_core_head_table_long  || ' core_head ' ;


   if i_core_vers_table_long is not null  then
      v_statement :=  v_statement || ' ,' ||  i_core_vers_table_long  || ' core_vers ';
   end if;

   v_statement :=  v_statement || ' where core_head.dwh_source_id='   || i_source_id  || ' ' ;

   -- DWH_SOURCE-KEY columns    (join is done for the case that cleanse is named DWH_SOURCE_KEY and core DWH_SOURCE_KEY_1
   for r in (SELECT   col_core_head.column_name     column_name_core_head
                     ,col_source.column_name        column_name_source
             FROM     all_tab_columns                      col_core_head
                     ,all_tab_columns                      col_source
             WHERE    col_core_head.owner      =  v_core_owner
             AND      col_core_head.table_name =  v_core_head_table
             AND      col_core_head.column_name LIKE ('DWH_SOURCE_KEY%')
             AND      col_source.owner         =  v_source_owner
             AND      col_source.table_name    =  v_source_table
             AND      col_source.column_name LIKE ('DWH_SOURCE_KEY%')
             AND      DECODE(col_core_head.column_name, 'DWH_SOURCE_KEY',     'DWH_SOURCE_KEY'
                                                       ,'DWH_SOURCE_KEY_1',   'DWH_SOURCE_KEY'
                                                       ,col_core_head.column_name) =
                      DECODE(col_source.column_name   , 'DWH_SOURCE_KEY',     'DWH_SOURCE_KEY'
                                                       ,'DWH_SOURCE_KEY_1',   'DWH_SOURCE_KEY'
                                                       ,col_source.column_name)
             ORDER BY col_core_head.column_name) loop

     v_statement :=  v_statement ||
                     ' and core_head.' || r.column_name_core_head || '=' || 'source.' || r.column_name_source;
   end loop;

   --  join with version-table, if exists ;
   --        and take the latest version
   --        assumption: load-order is always from past to future (chronological)
   if i_core_vers_table_long is not null  then
     v_statement :=  v_statement ||
                       ' and core_head.dwh_id=core_vers.dwh_head_id '
                          ||
                       ' and core_vers.dwh_valid_from=(select max(core_vers_1.dwh_valid_from) '   ||
                                                      ' from ' || i_core_vers_table_long  || ' core_vers_1 ' ||
                                                      ' where  core_vers_1.dwh_head_id=core_vers.dwh_head_id)';

   end if;



   -- dwh-foreign key columns    (loop across core-table columns )

   for r in c_dwh_fk_columns   (v_core_head_table) loop

     v_statement_function_call := 'godw_admin.godw_core_loader.get_fk_replaced' ||
                                 '('
                                     || '''' || v_source_table_long    || ''''   ||
                                 ',' || '''' || v_core_owner           || ''''   ||
                                 ',' || '''' || v_core_head_table      || ''''   ||
                                 ',' || '''' || r.column_name          || ''''   ||
                                 ',' || '''' || r.nullable             || ''''   ||
                                 ',' || '''' || i_fk_mapping_head_table    || ''''   ||
                                 ',' || 'source.rowid'                               ||
                                 ')' ;

     v_statement :=  v_statement || ' and (core_head.' || r.column_name || '=' ||  v_statement_function_call ;

     if r.nullable = 'Y' then
        v_statement :=  v_statement || ' or (core_head.' || r.column_name || ' is null and ' ||
                                         v_statement_function_call || '=' || '''' || 'NULL' || '''' || ')';
     end if;

     v_statement :=  v_statement || ')';

   end loop;

   for r in c_dwh_fk_columns   (v_core_vers_table) loop

      v_statement_function_call := 'godw_admin.godw_core_loader.get_fk_replaced' ||
                                 '('
                                     || '''' || v_source_table_long    || ''''   ||
                                 ',' || '''' || v_core_owner           || ''''   ||
                                 ',' || '''' || v_core_vers_table      || ''''   ||
                                 ',' || '''' || r.column_name          || ''''   ||
                                 ',' || '''' || r.nullable             || ''''   ||
                                 ',' || '''' || i_fk_mapping_vers_table    || ''''   ||
                                 ',' || 'source.rowid'                               ||
                                 ')' ;

      v_statement :=  v_statement || ' and (to_char(core_vers.' || r.column_name || ')=' || v_statement_function_call; --13.09.2016: Jira task:IDWETL-61 - added to_char()

      if r.nullable = 'Y' then
         v_statement :=  v_statement || ' or (core_vers.' || r.column_name || ' is null and ' ||
                                          v_statement_function_call || '=' || '''' || 'NULL' || '''' || ')';
      end if;

      v_statement :=  v_statement || ')';



   end loop;


   -- normal columns  (loop across core-table columns)
   for r in   (SELECT   all_tab_columns.column_name
               FROM     all_tab_columns
               WHERE    all_tab_columns.table_name =  v_core_head_table
               AND      all_tab_columns.owner      =  v_core_owner
               AND      all_tab_columns.column_name NOT LIKE 'DWH%'
               ORDER BY all_tab_columns.column_id) loop

     v_statement :=  v_statement ||
                 ' and (( core_head.' || r.column_name || '=' ||
                    ' source.'        || r.column_name || ')'   ||
                    ' or (core_head.' || r.column_name || ' is null and ' ||
                           ' source.' || r.column_name || ' is null))';
   end loop;

   for r in   (SELECT   all_tab_columns.column_name
               FROM     all_tab_columns
               WHERE    all_tab_columns.table_name =  v_core_vers_table
               AND      all_tab_columns.owner      =  v_core_owner
               AND      all_tab_columns.column_name NOT LIKE 'DWH%'
               ORDER BY all_tab_columns.column_id) loop

     v_statement :=  v_statement ||
                 ' and (( core_vers.' || r.column_name || '=' ||
                    ' source.'        || r.column_name || ')'   ||
                    ' or (core_vers.' || r.column_name || ' is null and ' ||
                           ' source.' || r.column_name || ' is null))';
   end loop;



   v_statement :=  v_statement || ')';


   RETURN  v_statement;

  END  get_exists_in_core;






  FUNCTION check_source_data_in_the_past
                              (i_source_id                   NUMBER
                              ,i_source_table_valid_from     VARCHAR2) RETURN VARCHAR2   IS



    v_statement                             VARCHAR2(32767);

    v_result                                VARCHAR2(4000);

  BEGIN



   -- **********************************************************************
   -- Check:  if source-data is in the past, related to core-area data
   -- **********************************************************************

   v_statement := 'select count(1) from ' || v_source_table_long  || ' source '                                  ||
                  ' where exists (select 1 from ' || v_core_head_table_long  || ' core_head '                    ||
                                              ',' || v_core_vers_table_long  || ' core_vers ';

   v_statement :=  v_statement || ' where core_head.dwh_source_id='   || i_source_id  || ' ' ;

   -- DWH_SOURCE-KEY columns    (join is done for the case that cleanse is named DWH_SOURCE_KEY and core DWH_SOURCE_KEY_1
   for r in (SELECT   col_core_head.column_name     column_name_core_head
                     ,col_source.column_name        column_name_source
             FROM     all_tab_columns                      col_core_head
                     ,all_tab_columns                      col_source
             WHERE    col_core_head.owner      =  v_core_owner
             AND      col_core_head.table_name =  v_core_head_table
             AND      col_core_head.column_name LIKE ('DWH_SOURCE_KEY%')
             AND      col_source.owner         =  v_source_owner
             AND      col_source.table_name    =  v_source_table
             AND      col_source.column_name LIKE ('DWH_SOURCE_KEY%')
             AND      DECODE(col_core_head.column_name, 'DWH_SOURCE_KEY',     'DWH_SOURCE_KEY'
                                                       ,'DWH_SOURCE_KEY_1',   'DWH_SOURCE_KEY'
                                                       ,col_core_head.column_name) =
                      DECODE(col_source.column_name   , 'DWH_SOURCE_KEY',     'DWH_SOURCE_KEY'
                                                       ,'DWH_SOURCE_KEY_1',   'DWH_SOURCE_KEY'
                                                       ,col_source.column_name)
             ORDER BY col_core_head.column_name) loop

     v_statement :=  v_statement ||
                     ' and core_head.' || r.column_name_core_head || '=' || 'source.' || r.column_name_source;
   end loop;

   --  join with version-table, and take the latest version
   v_statement :=  v_statement                                                                                ||
                       ' and core_head.dwh_id=core_vers.dwh_head_id '                                         ||
                       ' and core_vers.dwh_valid_from=(select max(core_vers_1.dwh_valid_from) '               ||
                                                      ' from ' || v_core_vers_table_long  || ' core_vers_1 '  ||
                                                      ' where  core_vers_1.dwh_head_id=core_vers.dwh_head_id)';

   -- now compare valid-from from the source, with the core-version-row

    v_statement :=  v_statement                                                                                ||
                    ' and core_vers.dwh_valid_from > source.' || i_source_table_valid_from || ')';



    debug(50,'statement for checking source-data is in the past',v_statement);
    execute immediate v_statement  into  v_result;


    if v_result = '0' then
       RETURN NULL;
    else
      RETURN  v_result;
    end if;

  END  check_source_data_in_the_past;







  PROCEDURE set_debug_level(i_debug_level      NUMBER) IS
  BEGIN
     godw_core_loader.v_debug_level := i_debug_level;
  END set_debug_level;




  PROCEDURE  load_table   (i_load_id                       NUMBER
                         , i_source_id                     NUMBER
                         , i_source_table                  VARCHAR2
                         , i_source_table_valid_from       VARCHAR2   DEFAULT NULL
                         , i_source_table_valid_to         VARCHAR2   DEFAULT NULL
                         , i_core_head_table               VARCHAR2
                         , i_fk_mapping_head_table         VARCHAR2   DEFAULT NULL
                         , i_fk_mapping_vers_table         VARCHAR2   DEFAULT NULL) IS



    TYPE              cur_typ IS REF CURSOR;
    v_cursor_source   cur_typ;

    v_statement                      varchar2(32767);
    v_id_string                      varchar2(100);

    v_flag_new_header                varchar2(1);

    v_source_rowid                   rowid;
    v_dwh_source_key_1               varchar2(50);
    v_dwh_source_key_2               varchar2(50);
    v_dwh_source_key_3               varchar2(50);
    v_dwh_source_key_4               varchar2(50);
    v_dwh_source_key_5               varchar2(50);

    v_core_head_rowid                rowid;

    v_dwh_head_id                    number;
    v_dwh_vers_id                    number;

    v_column_value_string            varchar2(4000);

    v_counter                        number;

    v_current_valid_from             date  := godw_utility.get_actual_valid_from(i_load_id);


    v_head_col_list_with_markers   VARCHAR2(4000);
    v_head_col_list                VARCHAR2(4000);
    v_vers_col_list_with_markers   VARCHAR2(4000);
    v_vers_col_list                VARCHAR2(4000);

    v_rows_source                  NUMBER     := 0;
    v_rows_source_delta_detected   NUMBER     := 0;
    v_rows_ins_head                NUMBER     := 0;
    v_rows_upd_head                NUMBER     := 0;
    v_rows_ins_vers                NUMBER     := 0;
    v_rows_upd_vers                NUMBER     := 0;


    v_valid_from                  DATE;
    v_valid_to                    DATE;

    v_dwh_vers_id_existing        NUMBER;
    v_change_of_version_happened  VARCHAR2(1);
    v_update_of_version_happened  VARCHAR2(1);

    v_valid_from_existing         DATE;
    v_valid_to_existing           DATE;

    v_log_id                      NUMBER;

    v_source_key_list             VARCHAR2(4000);
    v_order_by                    VARCHAR2(4000);

    v_debug_level_save            NUMBER;
    
    v_lookup_table                VARCHAR2(30);
    v_lookup_tables_string        VARCHAR2(4000);
    v_pos                         NUMBER;
    

    v_error                       VARCHAR2(4000);


  BEGIN


    v_current_load_id := i_load_id;


    -- ****************************************
    -- ***  Init Core Loader Log **************
    -- ****************************************

    delete from godw_core_loader_log
    where       godw_core_loader_log.load_id      = i_load_id
    and         godw_core_loader_log.source_id    = i_source_id
    and         godw_core_loader_log.source_table = i_source_table;

    select    SQ_GODW_CORE_LOADER_LOG.nextval into v_log_id from dual;

    insert into godw_core_loader_log
             (log_id
             ,load_id
             ,source_id
             ,source_table
             ,core_head_table
             ,start_parameter
             ,load_start
             )
    values (/* log_id          */  v_log_id
           ,/* load_id         */  i_load_id
           ,/* source_id       */  i_source_id
           ,/* source_table    */  i_source_table
           ,/* core_head_table */  i_core_head_table
           ,/* start_parameter */  'i_source_table_valid_from = '     ||  i_source_table_valid_from   || ';'
                               ||  'i_source_table_valid_to = '       ||  i_source_table_valid_to     || ';'
                               ||  'i_fk_mapping_head_table = '       ||  i_fk_mapping_head_table     || ';'
                               ||  'i_fk_mapping_vers_table = '       ||  i_fk_mapping_vers_table     || ';'
           ,/* load_start      */   systimestamp
           );

    debug(10,'Core Loader started');
    commit;

    -- **********************************
    -- ***  Define variables ************
    -- **********************************
    v_source_table_long              := UPPER(i_source_table);
    v_source_table                   := SUBSTR(v_source_table_long,INSTR(v_source_table_long,'.') + 1);
    v_source_owner                   := SUBSTR(v_source_table_long,1,INSTR(v_source_table_long,'.')-1);

    v_source_name                    := SUBSTR(v_source_owner,1,INSTR(v_source_owner,'_') - 1);
    v_core_owner                     := godw_utility.get_core_owner_with_source(v_source_name);



    v_core_head_table                := UPPER(i_core_head_table);
    v_core_vers_table                := UPPER(i_core_head_table || '_VERS');
    v_core_vers_table_for_sequence   := UPPER(i_core_head_table || '_V');      -- special name for generating the sequence-name of the version-tables
    v_core_head_table_long           := UPPER(v_core_owner     || '.' || v_core_head_table);
    v_core_vers_table_long           := UPPER(v_core_owner     || '.' || v_core_vers_table);


    v_head_col_list_with_markers     :=  get_column_list_string (i_owner           =>  v_core_owner
                                                                ,i_table_name      =>  v_core_head_table
                                                                ,i_use_marker      =>  'Y'
                                                                ,i_column_selector =>  'ALL');

    v_head_col_list                  :=  REPLACE(v_head_col_list_with_markers,'#','');


    v_vers_col_list_with_markers     :=  get_column_list_string (i_owner           =>  v_core_owner
                                                                ,i_table_name      =>  v_core_vers_table
                                                                ,i_use_marker      =>  'Y'
                                                                ,i_column_selector =>  'ALL');

    v_vers_col_list                  :=  REPLACE(v_vers_col_list_with_markers,'#','');



    -- get for statistics the number of rows in the source-table

    execute immediate 'select count(1) from ' || v_source_table_long  into v_rows_source;

    UPDATE  godw_core_loader_log
    SET     godw_core_loader_log.rows_source = v_rows_source
    WHERE   godw_core_loader_log.log_id      = v_log_id;

    COMMIT;


-- ***************
-- special-issue:  29-march-2010: coordinate with andreas mengel:  column_name renamed in
--      table:  co_functional_area_vers
--    this code has to be removed, after the table is corected

--IF v_core_vers_table  = 'CO_WORK_PACKAGE_AREA_VERS' THEN
--  v_vers_col_list_with_markers :=  REPLACE(v_vers_col_list_with_markers,'WP_AREA_DESC_SHORT','WORK_PACKAGE_AREA_DESC_SHORT');
--  v_vers_col_list_with_markers :=  REPLACE(v_vers_col_list_with_markers,'WP_AREA_DESC_LONG ','WORK_PACKAGE_AREA_DESC_LONG' );
--END IF ;

--debug(50,'manipulated v_vers_col_list_with_markers ' || v_vers_col_list_with_markers);


-- ***************


    v_statement := ' select all_tables.table_name'
               ||  ' from   all_tables'
               ||  ' where  all_tables.owner='      || '''' || v_core_owner      || ''''
               ||  ' and    all_tables.table_name=' || '''' || v_core_vers_table  || '''';
    BEGIN
      execute immediate v_statement into v_core_vers_table ;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        debug(10,'No Version table defined',v_statement);  -- No versioning required, we just have a head-table
        v_core_vers_table       := '';
        v_core_vers_table_long  := '';
    END;


    -- added at 28-july by jochen rapp:  "bulk-load", when no version-table exists and the core-header table is empty

    IF v_core_vers_table IS NULL    THEN

      v_statement := 'select count(1) '  ||
                     ' from ' || v_core_head_table_long;

      execute immediate v_statement INTO v_counter;

      if v_counter = 0 then


         -- ok, now we can do a acomplete header-load

         debug(10,'Complete header load can be done');

         v_statement             := 'INSERT INTO ' || v_core_head_table_long  || ' SELECT ';
         v_counter               := 0;
         v_lookup_tables_string  := NULL;
         for r in (SELECT   col_core_head.column_name
                           ,col_core_head.nullable
                   FROM     all_tab_columns               col_core_head
                   WHERE    col_core_head.owner      =  v_core_owner
                   AND      col_core_head.table_name =  v_core_head_table
                   ORDER BY col_core_head.column_id) loop
            v_counter := v_counter + 1;
            if v_counter > 1 then
               v_statement := v_statement || ',';
            end if;

            v_column_value_string := r.column_name;
            if     v_column_value_string  = 'DWH_ID' then
                   v_column_value_string :=  v_core_owner || '.sq_' || SUBSTR(v_core_head_table,4) || '.NEXTVAL';
            elsif  v_column_value_string  = 'DWH_SOURCE_ID' then
                   v_column_value_string :=  i_source_id;

            elsif  v_column_value_string  = 'DWH_SOURCE_KEY' and 
                      get_column_exists  (i_owner        => v_source_owner
                                         ,i_table_name   => v_source_table
                                         ,i_column_name  => v_column_value_string) = 'N' then

                   v_column_value_string := v_source_table || '.' || 'DWH_SOURCE_KEY_1';
                   

            elsif  v_column_value_string  = 'DWH_SOURCE_KEY_1' and 
                   get_column_exists  (i_owner        => v_source_owner
                                         ,i_table_name   => v_source_table
                                         ,i_column_name  => v_column_value_string) = 'N' then

                   v_column_value_string := v_source_table || '.' || 'DWH_SOURCE_KEY';


            --elsif  v_column_value_string  = 'DWH_SOURCE_KEY' then   hier eventuell noch dwh_source_key und _1 mappen
            --       v_column_value_string :=  _source_id;
            elsif  v_column_value_string  = 'DWH_CR_LOAD_ID'  then
                   v_column_value_string :=       i_load_id;
            elsif  v_column_value_string  = 'DWH_CR_JOB'      then
                   v_column_value_string :=  '''' ||   c_job_core_loader || '''';
            elsif  v_column_value_string  = 'DWH_UP_LOAD_ID'  then
                   v_column_value_string :=       'null';
            elsif  v_column_value_string  = 'DWH_UP_JOB'      then
                   v_column_value_string :=       'null';


            elsif  v_column_value_string like 'DWH%ID'        then

                   v_column_value_string := 'godw_admin.godw_core_loader.get_fk_replaced' ||
                                             '('
                                                  || '''' || v_source_table_long        || ''''   ||
                                              ',' || '''' || v_core_owner               || ''''   ||
                                              ',' || '''' || v_core_head_table          || ''''   ||
                                              ',' || '''' || r.column_name              || ''''   ||
                                              ',' || '''' || r.nullable                 || ''''   ||
                                              ',' || '''' || i_fk_mapping_head_table    || ''''   ||
                                              ',' || 'rowid'                            ||
                                             ')' ;

                  -- for join: get lookup-table

                  v_lookup_table :=  godw_common.get_reference_table
                                                 (i_owner       => v_core_owner
                                                 ,i_table_name  => v_core_head_table
                                                 ,i_column_name => r.column_name);

                  IF v_lookup_tables_string IS NOT NULL THEN
                     v_lookup_tables_string := v_lookup_tables_string || ',' ;
                  END IF;
                  v_lookup_tables_string := v_lookup_tables_string || v_lookup_table;

                  v_column_value_string := NULL;
                  IF r.nullable = 'Y' THEN
                     v_column_value_string := 'NVL(';
                  END IF;
                  v_column_value_string := v_column_value_string ||  v_lookup_table || '.' || 'DWH_ID';
                  IF r.nullable = 'Y' THEN
                     v_column_value_string := v_column_value_string || ',-1)';
                  END IF;
       
            else  -- normal column
               v_column_value_string := v_source_table || '.' || r.column_name;
            end if;

            v_statement := v_statement || v_column_value_string;

         end loop;


         v_statement := v_statement || ' from ' ||  v_source_table_long;


         if v_lookup_tables_string is not null then
            v_statement := v_statement || ',' || v_lookup_tables_string;


            v_counter := 0;
            FOR r in c_split_string (upper(i_fk_mapping_head_table),',') LOOP
                v_counter := v_counter + 1;
                if v_counter = 1 then
                   v_statement := v_statement || ' where ';
                else
                   v_statement := v_statement || ' and ';
                end if;

                v_pos := INSTR(r.splitted_string,'=>') ;

                v_statement := v_statement ||  v_source_table || '.' || substr(r.splitted_string,1,v_pos - 1) ||
                                ' = ' || godw_common.get_reference_table
                                          (i_owner       => v_core_owner
                                          ,i_table_name  => v_core_head_table
                                          ,i_column_name => substr(r.splitted_string,v_pos + 2)) ||
                                      '.' ||  'DWH_SOURCE_KEY_1';
                -- limit to current-core                          
                v_statement := v_statement || ' and ' ||
                               godw_common.get_reference_table
                                          (i_owner       => v_core_owner
                                          ,i_table_name  => v_core_head_table
                                          ,i_column_name => substr(r.splitted_string,v_pos + 2)) 
                                          || '.' ||  'DWH_SOURCE_ID' || ' = ' || i_source_id;                  



             END LOOP;


         end if;

         debug(50,'Complete header load',v_statement);
         
         v_counter := 0;
         execute immediate 'declare x number; begin ' || v_statement || '; :x := sql%rowcount; end;' using OUT v_counter;
         --
         -- statistics
         v_rows_source                 := v_counter;
         v_rows_source_delta_detected  := v_counter;
         v_rows_ins_head               := v_counter;
         v_rows_upd_head               := 0;
         v_rows_ins_vers               := 0;
         v_rows_upd_vers               := 0;
 
         goto end_of_load;
      end if;

    END IF;






    -- ******************************************
    -- ***  Loop through the source table  ******
    -- ******************************************

    v_statement := 'select rowid ' ;
    v_counter   := 0;
    for r in (SELECT   all_tab_columns.column_name
             FROM     all_tab_columns
             WHERE    all_tab_columns.table_name =  v_source_table
             AND      all_tab_columns.owner      =  v_source_owner
             AND      all_tab_columns.column_name LIKE ('DWH_SOURCE_KEY%')
             ORDER BY all_tab_columns.column_name) loop
      v_counter          := v_counter + 1;
      if v_counter >  1 then
         v_source_key_list := v_source_key_list || ',';
      end if;
      v_source_key_list := v_source_key_list || r.column_name;
      --v_statement := v_statement || ',' || r.column_name;

    end loop;
    if v_source_key_list is not null then
      v_statement := v_statement || ',' || v_source_key_list;
    end if;
    for i in (v_counter + 1)..5 loop
       v_statement := v_statement || ',' || 'NULL';
    end loop;

    v_statement := v_statement || ' ' ;


    if i_source_table_valid_from is not null then
       v_statement := v_statement || ',' || i_source_table_valid_from;
    end if;
    if i_source_table_valid_to is not null then
       v_statement := v_statement || ',' || i_source_table_valid_to;
    end if;

    v_statement := v_statement || ' from ' ||  v_source_table_long || ' source ';





-- *************************************


  v_statement :=  v_statement || ' where ' ||
    get_exists_in_core
                          (i_source_id              => i_source_id
                          ,i_source_table_long      => v_source_table_long
                          ,i_core_head_table_long   => v_core_head_table_long
                          ,i_core_vers_table_long   => v_core_vers_table_long
                          ,i_fk_mapping_head_table  => i_fk_mapping_head_table
                          ,i_fk_mapping_vers_table  => i_fk_mapping_vers_table
                          )   ;


-- achtung, durch exists funktion wird v_statement in einigen faellen groesser als 4000 zeichen,
--     SQL kann nur 4000 zeichen handhaben, PL/SQL mehr
--     siehe metalink-note: 300268
--    resultat:  ORA-1461
/*
  insert into godw_core_loader_debug
                (load_id  ,line_no                          ,date_created,description   ,  statement,ins_user,ins_date  ,upd_user,upd_date)
         values (i_load_id,sq_godw_core_loader_debug.nextval,systimestamp,'test jochen'
         ,v_statement,  user,sysdate   ,user    ,sysdate);

commit;
*/



-- *************************************


    -- changed at 5-may-2010,
    --    try to fix the issue with loading exchange-rates,
    --    when all the history is in 1 file the valid_from/valid_to in the core-table was incorrect

    if v_source_key_list is not null then
       v_order_by :=  v_source_key_list;
    end if;
    if i_source_table_valid_from is not null then
       if v_order_by is not null then
          v_order_by := v_order_by || ',';
       end if;
       v_order_by := v_order_by || i_source_table_valid_from;
    end if;

    if v_order_by is not null then
       v_statement := v_statement || ' order by ' || v_order_by ;
    end if;




    -- check for supported load-scenario:  core-loader can not load in the past
    --             it is not possible to load data from the source-table, that is older
    --             than the existing data in the core-version table
    if  i_source_table_valid_from   is not null   and
        i_source_table_valid_to     is not null   and
        v_core_vers_table_long      is not null   then

        v_error := check_source_data_in_the_past
                          (i_source_id                => i_source_id
                          ,i_source_table_valid_from  => i_source_table_valid_from
                          );

        if v_error is not null then
            raise_application_error (-20778,
                                    'Error in load scenario: tried to load ' || v_error ||
                                    ' rows in the past ');
        end if;

    end if;







    debug(50,'loop through source',v_statement);


    OPEN  v_cursor_source  FOR v_statement;
    LOOP

      -- *************************************
      -- ***  Fetch new row  *****************
      -- *************************************
      v_flag_new_header := 'N';
      v_dwh_head_id     := NULL;


      v_debug_level_save             := v_debug_level;
      GODW_CORE_LOADER.v_debug_level := 0;  -- reset debug-level to 0,otherwise all the debug-messages from the
                                            -- foreign-key functions will be written to the trace-table


      if     i_source_table_valid_from is null   and
             i_source_table_valid_to   is null   then

             FETCH     v_cursor_source INTO v_source_rowid,v_dwh_source_key_1
                                                          ,v_dwh_source_key_2
                                                          ,v_dwh_source_key_3
                                                          ,v_dwh_source_key_4
                                                          ,v_dwh_source_key_5;
             EXIT WHEN v_cursor_source%NOTFOUND;

      elsif  i_source_table_valid_from is not null   and
             i_source_table_valid_to   is     null   then

             FETCH     v_cursor_source INTO v_source_rowid,v_dwh_source_key_1
                                                          ,v_dwh_source_key_2
                                                          ,v_dwh_source_key_3
                                                          ,v_dwh_source_key_4
                                                          ,v_dwh_source_key_5
                                                          ,v_valid_from;
             EXIT WHEN v_cursor_source%NOTFOUND;

      elsif  i_source_table_valid_from is     null   and
             i_source_table_valid_to   is not null   then

             FETCH     v_cursor_source INTO v_source_rowid,v_dwh_source_key_1
                                                          ,v_dwh_source_key_2
                                                          ,v_dwh_source_key_3
                                                          ,v_dwh_source_key_4
                                                          ,v_dwh_source_key_5
                                                          ,v_valid_to;
             EXIT WHEN v_cursor_source%NOTFOUND;

      elsif  i_source_table_valid_from is not null   and
             i_source_table_valid_to   is not null   then

             FETCH     v_cursor_source INTO v_source_rowid,v_dwh_source_key_1
                                                          ,v_dwh_source_key_2
                                                          ,v_dwh_source_key_3
                                                          ,v_dwh_source_key_4
                                                          ,v_dwh_source_key_5
                                                          ,v_valid_from
                                                          ,v_valid_to;

               EXIT WHEN v_cursor_source%NOTFOUND;
      end if;


      GODW_CORE_LOADER.v_debug_level := v_debug_level_save; --  reset debug-level to orgiginal value



      v_rows_source_delta_detected := v_rows_source_delta_detected + 1;

      debug(50,'in source-loop row-number: ' || v_rows_source_delta_detected );




      -- log the information to allow follow-up of work-progress in table godw_core_loader_log
      if mod( v_rows_source_delta_detected,v_log_interval_rows) = 0 then
        update_work_progress  (i_log_id           => v_log_id
                              ,i_number_of_rows   => v_rows_source_delta_detected);
      end if;





      v_dwh_source_key_1 := get_escape_string(v_dwh_source_key_1);
      v_dwh_source_key_2 := get_escape_string(v_dwh_source_key_2);
      v_dwh_source_key_3 := get_escape_string(v_dwh_source_key_3);
      v_dwh_source_key_4 := get_escape_string(v_dwh_source_key_4);
      v_dwh_source_key_5 := get_escape_string(v_dwh_source_key_5);

      debug(50,'Source rowid is: ' || v_source_rowid,NULL );



      -- ******************************************
      -- ***  Determine the valid_from/valid_to ***
      -- ******************************************

      if v_valid_from is null  then
         v_valid_from  := v_current_valid_from;
      end if;
      if v_valid_to is null  then
         v_valid_to    := c_max_valid_to;
      end if;


      -- ******************************************
      -- ***  Get an existing core header ***
      -- ******************************************

      v_statement :=  'select rowid,dwh_id from ' || v_core_head_table_long || ' core_head  '
                 ||   ' where core_head.dwh_source_id = '     ||    i_source_id;
--                 ||   ' and   core_head.dwh_source_key = ' || '''' || v_dwh_source_key || '''';
      v_counter := 0;
      for c in ( SELECT   all_tab_columns.*
                 FROM     all_tab_columns
                 WHERE    all_tab_columns.table_name =  v_core_head_table
                 AND      all_tab_columns.owner      =  v_core_owner
                 AND      all_tab_columns.column_name LIKE ('DWH_SOURCE_KEY%')
                 -- ORDER BY  all_tab_columns.column_id    changed at 30-may-2010
                 ORDER BY all_tab_columns.column_name
                ) loop
         v_counter := v_counter + 1;

         -- changed at 25-may-2010: allow for optional source-keys (value of source-key is null)
         --                         DWH_SOURCE_KEY_1 may not be null, only DWH_SOURCE_KEY_2,3,4,5
         if     v_counter =  1 then
            v_statement := v_statement || ' and core_head.' || c.column_name || ' = ' || '''' || v_dwh_source_key_1 || '''';
         elsif  v_counter =  2 then
            if v_dwh_source_key_2 is not null then
              v_statement := v_statement || ' and core_head.' || c.column_name || ' = ' || '''' || v_dwh_source_key_2 || '''';
            else
              v_statement := v_statement || ' and core_head.' || c.column_name || ' is null ';
            end if;
         elsif  v_counter =  3 then
            if v_dwh_source_key_3 is not null then
              v_statement := v_statement || ' and core_head.' || c.column_name || ' = ' || '''' || v_dwh_source_key_3 || '''';
            else
              v_statement := v_statement || ' and core_head.' || c.column_name || ' is null ';
            end if;
         elsif  v_counter =  4 then
            if v_dwh_source_key_4 is not null then
              v_statement := v_statement || ' and core_head.' || c.column_name || ' = ' || '''' || v_dwh_source_key_4 || '''';
            else
              v_statement := v_statement || ' and core_head.' || c.column_name || ' is null ';
            end if;
         elsif  v_counter =  5 then
            if v_dwh_source_key_5 is not null then
              v_statement := v_statement || ' and core_head.' || c.column_name || ' = ' || '''' || v_dwh_source_key_5 || '''';
            else
              v_statement := v_statement || ' and core_head.' || c.column_name || ' is null ';
            end if;
         end if;
      end loop;


      BEGIN
        debug(50,'statement get core-head rowid',v_statement);
        execute immediate v_statement  into  v_core_head_rowid,v_dwh_head_id;
        v_flag_new_header := 'N';
        debug(50,'core header already exists ' || v_core_head_rowid,NULL);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          v_core_head_rowid := NULL;
          v_flag_new_header := 'Y';
          debug(50,'core header does NOT already exist',NULL);
      END;



      -- *******************************************************************************
      -- ***  1)  No Core row exists  **************************************************
      -- *******************************************************************************

      IF v_flag_new_header  = 'Y' THEN

        -- *********************************
        -- ***   Create new Header row   ***
        -- *********************************

        v_column_value_string := v_head_col_list_with_markers;
        debug(50,'v_column_value_string= ' || v_column_value_string,NULL);

--select user into v_statement from dual;
--debug(50,'user is: ' || v_statement);

        v_dwh_head_id  := get_seq_id(SUBSTR(v_core_head_table,4));   -- get new pk

        -- Replace DWH-Columns
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_ID#'             ,v_dwh_head_id);
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_ID#'      ,i_source_id);
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_KEY#'     ,'''' ||  v_dwh_source_key_1 || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_KEY_1#'   ,'''' ||  v_dwh_source_key_1 || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_KEY_2#'   ,'''' ||  v_dwh_source_key_2 || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_KEY_3#'   ,'''' ||  v_dwh_source_key_3 || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_KEY_5#'   ,'''' ||  v_dwh_source_key_4 || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_SOURCE_KEY#'     ,'''' ||  v_dwh_source_key_5 || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_CR_LOAD_ID#'     ,i_load_id);
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_LOAD_ID#'     ,0);
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_CR_JOB#'         ,'''' || c_job_core_loader || '''');
        v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_JOB#'         ,'''' || '''');

       -- Replace FK-Columns:   todo  hier noch den code optimieren, das nicht fuer jede row das data-dictonary gelesen wird
       if i_fk_mapping_head_table is not null then
         v_column_value_string  :=  get_all_fks_replaced
                                      (i_column_list_string   =>  v_column_value_string
                                     , i_source_table_rowid   =>  v_source_rowid
                                     , i_core_table           =>  i_core_head_table
                                     , i_fk_mapping           =>  i_fk_mapping_head_table
                                      );
       end if;

       -- remove the rest of the markers
       v_column_value_string  := REPLACE(v_column_value_string,'#','');

       v_statement := 'insert into '  || v_core_head_table_long  || '(' || v_head_col_list || ') '  ||
                      ' select '      || v_column_value_string  ||
                      ' from '        || i_source_table         ||
                      ' where rowid=' || '''' || v_source_rowid || '''';

       debug(50,'statement for insert new header',v_statement);
       execute immediate v_statement;

       v_rows_ins_head := v_rows_ins_head + 1;


        -- **********************************
        -- ***   Create new  Version row  ***
        -- **********************************

       if v_core_vers_table IS NULL THEN
          goto no_version_tabble_1;
       end if;

       v_column_value_string :=  v_vers_col_list_with_markers;

       v_dwh_vers_id   := get_seq_id(SUBSTR(v_core_vers_table_for_sequence,4));   -- get new pk     special name for sequence-names of version-tables


       v_column_value_string := REPLACE(v_column_value_string,'#DWH_ID#'         ,v_dwh_vers_id);
       v_column_value_string := REPLACE(v_column_value_string,'#DWH_HEAD_ID#'    ,v_dwh_head_id);

       -- changed at 18-may-2010 jochen rapp,
       v_column_value_string := REPLACE(v_column_value_string,'#DWH_VALID_FROM#' ,
                                       'TO_DATE(''' || to_char(v_valid_from,c_format_date_time) ||
                                        ''',''' || c_format_date_time || ''')');
       v_column_value_string := REPLACE(v_column_value_string,'#DWH_VALID_TO#' ,
                                          'TO_DATE(''' || to_char(v_valid_to,c_format_date_time) ||
                                        ''',''' || c_format_date_time || ''')');

       v_column_value_string := REPLACE(v_column_value_string,'#DWH_CR_LOAD_ID#' ,i_load_id);
       v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_LOAD_ID#' ,0);
       v_column_value_string := REPLACE(v_column_value_string,'#DWH_CR_JOB#'     ,'''' || c_job_core_loader || '''');
       v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_JOB#'     ,'''' || '''');


       -- Replace FK-Columns:   todo  hier noch den code optimieren, das nicht fuer jede row das data-dictonary gelesen wird
       if i_fk_mapping_vers_table is not null then
          v_column_value_string  :=  get_all_fks_replaced
                                      (i_column_list_string   =>  v_column_value_string
                                     , i_source_table_rowid   =>  v_source_rowid
                                     , i_core_table           =>  v_core_vers_table
                                     , i_fk_mapping           =>  i_fk_mapping_vers_table
                                      );
       end if;

       -- remove the rest of the markers
       v_column_value_string := REPLACE(v_column_value_string,'#','');


       v_statement := 'insert into '  || v_core_vers_table_long  || '(' || v_vers_col_list || ') '  ||
                      ' select '      || v_column_value_string  ||
                      ' from '        || i_source_table         ||
                      ' where rowid=' || '''' || v_source_rowid || '''';

       debug(50,'statement for insert new version',v_statement);

       execute immediate v_statement;

       v_rows_ins_vers := v_rows_ins_vers + 1;

       <<no_version_tabble_1>>

       NULL;
    END IF;


    -- *******************************************************************************
    -- ***  2)  Core row exists  **************************************************
    -- *******************************************************************************
    IF v_flag_new_header  = 'N'  THEN

       -- *************************
       -- ***   Header-Table    ***
       -- *************************

       -- Detect if an  update on head-table exists (SCD-1 changes)

       debug(50,'check for an update of the header',NULL);

       v_statement :=  'select 1 '
                   ||  ' from '
                   ||   v_core_head_table_long  || ' core_head, '
                   ||   i_source_table          || ' source '
                   || ' where core_head.rowid = ' || '''' ||  v_core_head_rowid  ||  ''''
                   || ' and   source.rowid    = ' || '''' ||  v_source_rowid    ||  '''' ;

       v_counter := 0;
       FOR r_cols IN c_tab_cols  (v_core_owner
                                 ,i_core_head_table
                                 ,'WITHOUT_DWH_COLS') LOOP

          if  r_cols.column_name LIKE 'DWH%ID' then -- now we have a fk-column
               v_id_string  := get_fk_replaced
                                        (i_source_table_long  => v_source_table_long
                                        ,i_core_owner         => v_core_owner
                                        ,i_core_table         => v_core_head_table
                                        ,i_column_name        => r_cols.column_name
                                        ,i_column_nullable    => r_cols.nullable
                                        ,i_fk_mapping         => i_fk_mapping_head_table
                                        ,i_source_table_rowid => v_source_rowid
                                        );



              v_statement := v_statement || ' and (( core_head.'   || r_cols.column_name || ' = ' ||
                                                                      v_id_string        || ')'   ||
                                                 ' or (core_head.' || r_cols.column_name || ' is null and ' ||
                                                                      v_id_string        || ' is null))';

         else

            v_statement := v_statement || ' and (( core_head.'     || r_cols.column_name   || ' = ' ||
                                                    ' source.'     || r_cols.column_name   || ')'   ||
                                                 ' or (core_head.' || r_cols.column_name   || ' is null and ' ||
                                                        ' source.' || r_cols.column_name   || ' is null))';
          end if;

          v_counter := v_counter + 1 ;

       END LOOP;
       debug(50,'statement for check update of header',v_statement);

       begin
         execute immediate v_statement into v_counter;

         debug(50,'No update of the header detected');

       exception
         when no_data_found then
            v_counter := 0;
       end;


       IF v_counter = 0 THEN

          debug(50,'SCD1 change in Header happened for: ' || v_dwh_source_key_1 || ' ' ||
                                                             v_dwh_source_key_2 || ' ' ||
                                                             v_dwh_source_key_3 || ' ' ||
                                                             v_dwh_source_key_4 || ' ' ||
                                                             v_dwh_source_key_5
                                                               ,NULL );

          v_statement := 'UPDATE ' || v_core_head_table_long || ' SET (';

          v_column_value_string :=  get_column_list_string
                                (i_owner              =>  v_core_owner
                                ,i_table_name         =>  i_core_head_table
                                ,i_use_marker         => 'Y'
                                ,i_column_selector    => 'HEADER_SCD1');

           v_statement := v_statement || REPLACE(v_column_value_string,'#','') || ') = (select ';


-- **********************************************************
           -- Replace FK-Columns:   todo  hier noch den code optimieren, das nicht fuer jede row das data-dictonary gelesen wird
           if i_fk_mapping_head_table is not null then
             v_column_value_string  :=  get_all_fks_replaced
                                        (i_column_list_string   =>  v_column_value_string
                                       , i_source_table_rowid   =>  v_source_rowid
                                       , i_core_table           =>  v_core_head_table
                                       , i_fk_mapping           =>  i_fk_mapping_head_table
                                        );
           end if;

-- **********************************************************

           v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_LOAD_ID#' ,i_load_id);
           v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_JOB#'     ,'''' || c_job_core_loader || '''');

           v_column_value_string := REPLACE(v_column_value_string,'#','');


           v_statement := v_statement || v_column_value_string                                          ||
                                         ' from ' || v_source_table_long                                ||
                                         ' where rowid=' || '''' || v_source_rowid    || '''' || ') '   ||
                                         ' where rowid=' || '''' || v_core_head_rowid || '''';

           debug(40,'SCD1 header update statement',v_statement );

           v_counter := 0;
           execute immediate 'declare x number; begin ' || v_statement || '; :x := sql%rowcount; end;' using OUT v_counter;

           v_rows_upd_head := v_rows_upd_head + 1;


        END IF;


       -- *************************
       -- ***   Version-Table   ***
       -- *************************

       if v_core_vers_table IS NULL THEN
          goto no_version_tabble_2;
       end if;

       -- Detect if an  update on the version-table exists (SCD-2 changes)

       debug(50,'check for an update of the version',NULL);

       -- first detect the  from-/to range:
           -- v_source_valid_from,v_source_valid_to  is alredy defined

       -- get the current version row

       v_statement := 'select vers.dwh_id,vers.dwh_valid_from,vers.dwh_valid_to '  ||
                      ' from ' || v_core_vers_table_long || ' vers ' ||
                      ' where vers.dwh_head_id = '      ||             v_dwh_head_id          ||
                      ' and   vers.dwh_valid_from = (select max(vers_1.dwh_valid_from) ' ||
                                                   ' from ' ||v_core_vers_table_long || ' vers_1 ' ||
                                                   ' where vers_1.dwh_head_id = vers.dwh_head_id)';

       debug(50,'statement for getting the version-row',v_statement);

       begin
         execute immediate v_statement into v_dwh_vers_id_existing,v_valid_from_existing,v_valid_to_existing;
       exception
         when no_data_found then
            v_dwh_vers_id_existing := NULL;

            debug(50,'There is no active actual version',NULL);

            -- there is a header but not current-version  !
            --   (a version that is valid today)
            --  so the old versions are expired, and we just can insert the new version

       end;
       debug(50,'Existing version-row: ' || v_dwh_vers_id_existing,NULL);


       -- check if an update of the actual row exists
       IF  v_dwh_vers_id_existing IS NOT NULL THEN
           V_Statement :=  'select 1 '
                       ||  ' from '
                       ||   v_core_vers_table_long || ' core_vers, '
                       ||   i_source_table         || ' source '
                       || ' where core_vers.dwh_id = ' || v_dwh_vers_id_existing
                       || ' and   source.rowid     = ' || '''' ||  v_source_rowid    ||  '''' ;

           V_Counter := 0;
               for r_cols In C_Tab_Cols  (v_core_owner
                                         ,v_core_vers_table
                                         ,'WITHOUT_DWH_COLS') LOOP


              If  R_Cols.Column_Name Like 'DWH%ID' Then -- Now We Have A Fk-Column
                   v_id_string  := get_fk_replaced
                                        (i_source_table_long  => v_source_table_long
                                        ,i_core_owner         => v_core_owner
                                        ,i_core_table         => v_core_vers_table
                                        ,i_column_name        => r_cols.column_Name
                                        ,i_column_nullable    => r_cols.nullable
                                        ,i_fk_mapping         => i_fk_mapping_vers_table
                                        ,i_source_table_rowid => v_source_rowid
                                        );


                  v_statement := v_statement || ' and (( core_vers.'   || r_cols.Column_Name || ' = ' ||
                                                                          v_id_String        || ')'   ||
                                                     ' or (core_vers.' || r_cols.Column_Name || ' is null and ' ||
                                                                          v_id_String        || ' is null))';


              Else
                V_Statement := V_Statement || ' and (( core_vers.'     || R_Cols.Column_Name || ' = ' ||
                                                     ' source.'        || R_Cols.Column_Name   || ')'   ||
                                                     ' or (core_vers.' || R_Cols.Column_Name || ' is null and ' ||
                                                            ' source.' || R_Cols.Column_Name || ' is null))';
              End If;

              V_Counter := V_Counter + 1 ;

           End Loop;

           if length(V_Statement) > 4000 then
              Debug(50,'statment for check update of version',substr(V_Statement,1,3950) || '  ... statement truncated (too long)');
           else
              Debug(50,'statment for check update of version',V_Statement);
           end if;

           v_change_of_version_happened := 'N';
           Begin
             Execute Immediate V_Statement Into V_Counter;
           Exception
             When No_Data_Found Then
                v_change_of_version_happened := 'Y';
           end;

       END IF;



       v_update_of_version_happened := 'N';
       -- ****  Update of version-table happened: for example when you load data 2 times a day
       IF v_dwh_vers_id_existing IS NOT NULL             AND
          v_change_of_version_happened = 'Y'             AND
          v_valid_from_existing        = v_valid_from    AND
          v_valid_to_existing          = v_valid_to      THEN


          debug(40,'SCD2 change in Version happened and we have an update of the version for: ' ||
                                                                             v_dwh_source_key_1 || ' ' ||
                                                                             v_dwh_source_key_2 || ' ' ||
                                                                             v_dwh_source_key_3 || ' ' ||
                                                                             v_dwh_source_key_4 || ' ' ||
                                                                             v_dwh_source_key_5
                                                                            ,NULL );


          v_update_of_version_happened := 'Y';

          v_statement := 'UPDATE ' || v_core_vers_table_long || ' SET (';

          v_column_value_string :=  get_column_list_string
                                (i_owner              =>  v_core_owner
                                ,i_table_name         =>  v_core_vers_table
                                ,i_use_marker         => 'Y'
                                ,i_column_selector    => 'VERSION_SCD2');

           v_statement := v_statement || REPLACE(v_column_value_string,'#','') || ') = (select ';

           -- **********************************************************
           -- Replace FK-Columns:   todo  hier noch den code optimieren, das nicht fuer jede row das data-dictonary gelesen wird
           if i_fk_mapping_vers_table is not null then
             v_column_value_string  :=  get_all_fks_replaced
                                        (i_column_list_string   =>  v_column_value_string
                                       , i_source_table_rowid   =>  v_source_rowid
                                       , i_core_table           =>  v_core_vers_table
                                       , i_fk_mapping           =>  i_fk_mapping_vers_table
                                        );
           end if;

           -- **********************************************************


           v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_LOAD_ID#' ,i_load_id);
           v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_JOB#'     ,'''' || c_job_core_loader || '''');
           v_column_value_string := REPLACE(v_column_value_string,'#','');

           v_statement := v_statement || v_column_value_string                                           ||
                                         ' from ' || v_source_table_long                                 ||
                                         ' where rowid =' || '''' || v_source_rowid    || '''' || ') '   ||
                                         ' where dwh_id=' || '''' || v_dwh_vers_id_existing || '''';

           debug(40,'SCD2 version update statement',v_statement );

           v_counter := 0;
           execute immediate 'declare x number; begin ' || v_statement || '; :x := sql%rowcount; end;' using OUT v_counter;

           v_rows_upd_vers := v_rows_upd_vers + 1;


       END IF;



       -- *** Terminate the existing version-row (set the valid_to date)
       IF v_dwh_vers_id_existing IS NOT NULL   AND
          v_change_of_version_happened = 'Y'   AND
          v_update_of_version_happened = 'N'   THEN

          debug(40,'SCD2 change in Version happened for: ' || v_dwh_source_key_1 || ' ' ||
                                                              v_dwh_source_key_2 || ' ' ||
                                                              v_dwh_source_key_3 || ' ' ||
                                                              v_dwh_source_key_4 || ' ' ||
                                                              v_dwh_source_key_5
                                                               ,NULL );



          -- *** set the valid_to date for the existing row
          --   changed at 22-july-2010   according to  ://www.rkimball.com/html/08dt/KU100KeepYourKeysSimple.pdf
          --     be aware that you can not use BETWEEN for selects anymore.
          --     use in queries:  greater-than-or-equal for the begin date and less-than for the end date
          v_statement := 'UPDATE ' || v_core_vers_table_long || ' SET '
                            || ' dwh_valid_to = ' || 'TO_DATE(''' || to_char(v_valid_from
                                                                             ,c_format_date_time) ||
                                                                           ''',''' || c_format_date_time || ''')'
                            || ' where dwh_id = ' || v_dwh_vers_id_existing;

          debug(50,'SCD2 set valid_to of existing version row',v_statement );


          execute immediate 'declare x number; begin ' || v_statement || '; :x := sql%rowcount; end;' using OUT v_counter;

       END IF;



       -- ***  Create  a new version row  ***
       IF  v_update_of_version_happened = 'N'      AND
          (v_dwh_vers_id_existing  IS NULL    OR
           v_change_of_version_happened = 'Y')     THEN

          v_column_value_string :=  v_vers_col_list_with_markers;

          v_dwh_vers_id  := get_seq_id(SUBSTR(v_core_vers_table_for_sequence,4));   -- get new pk

          v_column_value_string := REPLACE(v_column_value_string,'#DWH_ID#'         ,v_dwh_vers_id);
          v_column_value_string := REPLACE(v_column_value_string,'#DWH_HEAD_ID#'    ,v_dwh_head_id);

          v_column_value_string := REPLACE(v_column_value_string,'#DWH_VALID_FROM#' ,
                                           'TO_DATE(''' || to_char(v_valid_from,c_format_date_time) ||
                                                              ''',''' || c_format_date_time || ''')');
          v_column_value_string := REPLACE(v_column_value_string,'#DWH_VALID_TO#' ,
                                           'TO_DATE(''' || to_char(v_valid_to,c_format_date_time) ||
                                                              ''',''' || c_format_date_time || ''')');

          v_column_value_string := REPLACE(v_column_value_string,'#DWH_CR_LOAD_ID#' ,i_load_id);
          v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_LOAD_ID#' ,0);
          v_column_value_string := REPLACE(v_column_value_string,'#DWH_CR_JOB#'     ,'''' || c_job_core_loader || '''');
          v_column_value_string := REPLACE(v_column_value_string,'#DWH_UP_JOB#'     ,'''' || '''');


          -- Replace FK-Columns:   todo  hier noch den code optimieren, das nicht fuer jede row das data-dictonary gelesen wird
          if i_fk_mapping_vers_table is not null then
             v_column_value_string  :=  get_all_fks_replaced
                                        (i_column_list_string   =>  v_column_value_string
                                       , i_source_table_rowid   =>  v_source_rowid
                                       , i_core_table           =>  v_core_vers_table
                                       , i_fk_mapping           =>  i_fk_mapping_vers_table
                                        );
          end if;

          -- remove the rest of the markers
          v_column_value_string := REPLACE(v_column_value_string,'#','');



          v_statement := 'insert into '  || v_core_vers_table_long  || '(' || v_vers_col_list || ') '  ||
                         ' select '      || v_column_value_string   ||
                         ' from '        || i_source_table          ||
                         ' where rowid=' || '''' || v_source_rowid  || '''';

          debug(50,'statement for insert new version',v_statement);
          execute immediate v_statement;

          v_rows_ins_vers := v_rows_ins_vers + 1;


       END IF;

       -- wenn keine veraenderung, dann nichts unternehmen

       -- wenn veraenderung vorliegt, dann valid-to, der bestehenenden row setzen
       --    und neue row anlegen


       <<no_version_tabble_2>>
       null;



    END IF;


    <<end_loop>>
    null;

  END LOOP;

  <<end_of_load>>

  update   godw_core_loader_log
  set      godw_core_loader_log.load_end                    = systimestamp
  ,        godw_core_loader_log.rows_source                 = v_rows_source
  ,        godw_core_loader_log.rows_source_delta_detected  = v_rows_source_delta_detected
  ,        godw_core_loader_log.rows_ins_head               = v_rows_ins_head
  ,        godw_core_loader_log.rows_upd_head               = v_rows_upd_head
  ,        godw_core_loader_log.rows_ins_vers               = v_rows_ins_vers
  ,        godw_core_loader_log.rows_upd_vers               = v_rows_upd_vers
  where    godw_core_loader_log.log_id                      = v_log_id;


  COMMIT;


END load_table;






END GODW_CORE_LOADER;

/

  GRANT EXECUTE ON "GODW_ADMIN"."GODW_CORE_LOADER" TO PUBLIC;
  GRANT EXECUTE ON "GODW_ADMIN"."GODW_CORE_LOADER" TO "TEST_CLEANSE";
  GRANT EXECUTE ON "GODW_ADMIN"."GODW_CORE_LOADER" TO "TEST_CORE";
![image](https://github.com/vikash24248/vikash24248/assets/155230910/0190dd12-0f2f-4231-bec6-5b36c8abc68d)

