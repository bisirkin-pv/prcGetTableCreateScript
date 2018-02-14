IF EXISTS(SELECT * FROM sys.objects WHERE OBJECT_ID('tool.prcGetTableCreateScript')=OBJECT_ID)
	DROP PROCEDURE tool.prcGetTableCreateScript
GO

CREATE PROCEDURE tool.prcGetTableCreateScript
(
	 @tableName SYSNAME
	,@base SYSNAME = ''
	,@bracket NVARCHAR(2) = ''
	,@flags  NVARCHAR(5) = '00000'
)
AS
BEGIN
SET NOCOUNT ON
    DECLARE 
         @objectName        SYSNAME
        ,@tempName          SYSNAME
        ,@objectId          INT
        ,@tab               NVARCHAR(10) = SPACE(4)
        ,@bracketStart      NVARCHAR(1)
        ,@bracketEnd        NVARCHAR(1)
        ,@isDrop            NVARCHAR(1)        
        ,@isIndex           NVARCHAR(1)
        ,@isInsert          NVARCHAR(1)
        ,@isUseDB           NVARCHAR(1)
        ,@isPrint           NVARCHAR(1)
    SELECT @bracketStart = SUBSTRING(@bracket,1,1)
    SELECT @bracketEnd   = SUBSTRING(@bracket,2,1)
    SELECT @isDrop       = IIF(SUBSTRING(@flags,1,1) = '',N'0',SUBSTRING(@flags,1,1))
    SELECT @isIndex      = IIF(SUBSTRING(@flags,2,1) = '',N'0',SUBSTRING(@flags,2,1))
    SELECT @isInsert     = IIF(SUBSTRING(@flags,3,1) = '',N'0',SUBSTRING(@flags,3,1))
    SELECT @isUseDB      = IIF(SUBSTRING(@flags,4,1) = '',N'0',SUBSTRING(@flags,4,1))
    SELECT @isPrint      = IIF(SUBSTRING(@flags,5,1) = '',N'0',SUBSTRING(@flags,5,1))

    SET @base = IIF(@base = '', db_name(), @base)

    SET @tempName = IIF(@base='tempdb', CONCAT(@base, '.dbo.', @tableName)
                                      , CONCAT(@base, '.'    , @tableName)
                       )
    

    DECLARE @SQLEXEC  NVARCHAR(MAX) = ''
    DECLARE @SQLEXEC2 NVARCHAR(MAX) = ''
    DECLARE @SQLEXEC3 NVARCHAR(MAX) = ''
    DECLARE @SQLEXEC4 NVARCHAR(MAX) = ''
    DECLARE @SQLEXEC5 NVARCHAR(MAX) = ''

    IF	EXISTS (SELECT * FROM tempdb.sys.objects WHERE OBJECT_ID('tempdb..#object')=OBJECT_ID)
		    DROP TABLE #object
    CREATE TABLE #object
    (
         objectName SYSNAME
        ,objectId INT
    )

    set @SQLEXEC = 'INSERT INTO #object
    SELECT 
          IIF(''' + @base + ''' != ''tempdb'', ''' +  @bracketStart +''' + s.name + ''' + @bracketEnd + '''+ ''.'','''') + ''' 
          + @bracketStart + ''' + SUBSTRING(o.NAME, 1, IIF(CHARINDEX(''___'', o.NAME)>0, CHARINDEX(''___'', o.NAME) - 1, LEN(o.name))) + ''' +@bracketEnd + ''' AS name
        , o.[object_id]
    FROM ' + @base + '.sys.objects o WITH (NOWAIT)
    JOIN ' + @base + '.sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
    WHERE 1=1
        AND o.object_id = OBJECT_ID(''' + @tempName + ''')
        AND o.[type] = ''U''
        AND o.is_ms_shipped = 0
    '

    EXEC (@SQLEXEC)

    IF(@isPrint = N'1')
        BEGIN
            PRINT @SQLEXEC
        END

    SELECT 
         @objectName = objectName
        ,@objectId = objectId
    FROM #object

    IF @objectId IS NULL
        BEGIN
        DECLARE @err VARCHAR(1000) = 'Объект ' + @base + '.' + @tableName + ' не найден.' 
           ; THROW 777777, @err, 1
        END

    SET @SQLEXEC = '
    DECLARE @SQL NVARCHAR(MAX) = ''''
    ;WITH index_column AS 
    (
        SELECT 
              ic.[object_id]
            , ic.index_id
            , ic.is_descending_key
            , ic.is_included_column
            , c.name
        FROM ' + @base + '.sys.index_columns ic WITH (NOWAIT)
        JOIN ' + @base + '.sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
        WHERE ic.[object_id] = ' + CAST(@objectId AS NVARCHAR(15)) + '
    ),
    fk_columns AS 
    (
         SELECT 
              k.constraint_object_id
            , cname = c.name
            , rcname = rc.name
        FROM ' + @base + '.sys.foreign_key_columns k WITH (NOWAIT)
        JOIN ' + @base + '.sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id 
        JOIN ' + @base + '.sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
        WHERE k.parent_object_id = ' + CAST(@objectId AS NVARCHAR(15)) + '
    ),maxLen AS 
    (
        SELECT 
             column_id
            ,maxLenColumn
            ,MAX(LEN(columnType)) OVER() AS maxColumnType
            ,columnType
        FROM  (
            SELECT 
                c.column_id
               ,MAX(LEN(c.name))  OVER() AS maxLenColumn
               ,UPPER(tp.name) + 
                        CASE WHEN tp.name IN (''varchar'', ''char'', ''varbinary'', ''binary'', ''text'')
                               THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length AS VARCHAR(5)) END + '')''
                             WHEN tp.name IN (''nvarchar'', ''nchar'', ''ntext'')
                               THEN ''('' + CASE WHEN c.max_length = -1 THEN ''MAX'' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + '')''
                             WHEN tp.name IN (''datetime2'', ''time2'', ''datetimeoffset'') 
                               THEN ''('' + CAST(c.scale AS VARCHAR(5)) + '')''
                             WHEN tp.name = ''decimal'' 
                               THEN ''('' + CAST(c.[precision] AS VARCHAR(5)) + '','' + CAST(c.scale AS VARCHAR(5)) + '')''
                            ELSE ''''
                        END AS columnType
            FROM ' + @base + '.sys.columns c
            JOIN ' + @base + '.sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
            WHERE object_id = ' + CAST(@objectId AS NVARCHAR(15)) + '
        ) c
    )'
    SET @SQLEXEC2  =  
    '
    SELECT @SQL = 
        IIF(' + @isUseDB + ' = 1, ''USE '' + ''' + @base + ''' + CHAR(13) + ''GO'' + CHAR(13), '''') + 
        IIF(' + @isDrop + ' = 1, ''IF	EXISTS (SELECT * FROM ' + @base + '.sys.objects WHERE OBJECT_ID(''+''''''' + @tempName + '''''''+'') = OBJECT_ID)  '' + CHAR(13) + ''' + @tab + '''+''DROP TABLE '' + '''+ @objectName +''' + CHAR(13), '''') +
        + ''CREATE TABLE '' + ''' + @objectName + ''' + CHAR(13) + ''('' + CHAR(13) + STUFF((
        SELECT ''' + @tab + ''' + '','' + ''' +  @bracketStart + '''+ c.name + ''' + @bracketEnd +''' + SPACE(mlc.maxLenColumn - LEN(c.name)+1) +
            CASE WHEN c.is_computed = 1
                THEN ''AS '' + cc.[definition] 
                ELSE columnType +
                    /*CASE WHEN c.collation_name IS NOT NULL THEN '' COLLATE '' + c.collation_name ELSE '''' END +*/
                    SPACE(mlc.maxColumnType - LEN(mlc.columnType)+1) + CASE WHEN c.is_nullable = 1 THEN ''NULL'' ELSE ''NOT NULL'' END +
                    CASE WHEN dc.[definition] IS NOT NULL THEN '' DEFAULT'' + dc.[definition] ELSE '''' END + 
                    CASE WHEN ic.is_identity = 1 THEN '' IDENTITY('' + CAST(ISNULL(ic.seed_value, ''0'') AS CHAR(1)) + '','' + CAST(ISNULL(ic.increment_value, ''1'') AS CHAR(1)) + '')'' ELSE '''' END 
            END + CHAR(13)
        FROM ' + @base + '.sys.columns c WITH (NOWAIT)
        JOIN ' + @base + '.sys.types tp WITH (NOWAIT) 
            ON c.user_type_id = tp.user_type_id
        LEFT JOIN ' + @base + '.sys.computed_columns cc WITH (NOWAIT) 
            ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
        LEFT JOIN ' + @base + '.sys.default_constraints dc WITH (NOWAIT) 
            ON c.default_object_id != 0 
            AND c.[object_id] = dc.parent_object_id 
            AND c.column_id = dc.parent_column_id
        LEFT JOIN ' + @base + '.sys.identity_columns ic WITH (NOWAIT) 
            ON c.is_identity = 1 
            AND c.[object_id] = ic.[object_id] 
            AND c.column_id = ic.column_id
        JOIN maxLen mlc ON c.column_id = mlc.column_id        
        WHERE c.[object_id] =  ' + CAST(@objectId AS NVARCHAR(15)) + '
        ORDER BY c.column_id
        FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+1, ''' + @tab + ''' + '' '')        
        + ISNULL((SELECT ''' + @tab + ''' + '',CONSTRAINT '' + ''' + @bracketStart + '''+ k.name +  ''' + @bracketEnd +'''  + '' PRIMARY KEY ('' + 
                        (SELECT STUFF((
                             SELECT '', '' + '''+ @bracketStart + '''+ c.name + ''' + @bracketEnd +''' + '' '' + CASE WHEN ic.is_descending_key = 1 THEN ''DESC'' ELSE ''ASC'' END
                             FROM ' + @base + '.sys.index_columns ic WITH (NOWAIT)
                             JOIN ' + @base + '.sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                             WHERE ic.is_included_column = 0
                                 AND ic.[object_id] = k.parent_object_id 
                                 AND ic.index_id = k.unique_index_id     
                             FOR XML PATH(N''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, ''''))
                + '')'' + CHAR(13)
                FROM ' + @base + '.sys.key_constraints k WITH (NOWAIT)
                WHERE k.parent_object_id = ' + CAST(@objectId AS NVARCHAR(15)) + ' 
                    AND k.[type] = ''PK''), '''') + '')''  + CHAR(13)
        '
      SET @SQLEXEC3 =         
        '+ IIF('+ @isIndex + ' = 1, 
        ISNULL((SELECT (
            SELECT CHAR(13) +
                 ''ALTER TABLE '' + ''' + @objectName + ''' + CHAR(13) + ''' + @tab + ''' + '' WITH'' 
                + CASE WHEN fk.is_not_trusted = 1 
                    THEN '' NOCHECK'' 
                    ELSE '' CHECK'' 
                  END + 
                  '' ADD CONSTRAINT '' + ''' + @bracketStart + ''' + fk.name  + ''' + @bracketEnd +''' + CHAR(13) + ''' + @tab + ''' + '' FOREIGN KEY('' 
                  + STUFF((
                    SELECT '','' + ''' + @bracketStart + ''' + k.cname + ''' + @bracketEnd +'''
                    FROM fk_columns k
                    WHERE k.constraint_object_id = fk.[object_id]
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, '''')
                   + '')'' +
                  '' REFERENCES '' + ''' + @bracketStart + ''' + SCHEMA_NAME(ro.[schema_id]) + ''' + @bracketEnd +''' + ''.'' + ''' + @bracketStart + ''' + ro.name + ''' + @bracketEnd +''' + '' (''
                  + STUFF((
                    SELECT '','' + ''' + @bracketStart + ''' + k.rcname + ''' + @bracketEnd +'''
                    FROM fk_columns k
                    WHERE k.constraint_object_id = fk.[object_id]
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, 2, '''')
                   + '')''
                + CASE 
                    WHEN fk.delete_referential_action = 1 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON DELETE CASCADE'' 
                    WHEN fk.delete_referential_action = 2 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON DELETE SET NULL''
                    WHEN fk.delete_referential_action = 3 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON DELETE SET DEFAULT'' 
                    ELSE '''' 
                  END
                + CASE 
                    WHEN fk.update_referential_action = 1 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON UPDATE CASCADE''
                    WHEN fk.update_referential_action = 2 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON UPDATE SET NULL''
                    WHEN fk.update_referential_action = 3 THEN CHAR(13) + ''' + @tab + ''' + ''' + @tab + ''' +'' ON UPDATE SET DEFAULT''  
                    ELSE '''' 
                  END 
                + CHAR(13) +  CHAR(13) + ''ALTER TABLE '' + ''' + @objectName + ''' + CHAR(13) + ''' + @tab + ''' + '' CHECK CONSTRAINT '' + ''' + @bracketStart + ''' + fk.name  + ''' + @bracketEnd +''' + CHAR(13)
            FROM ' + @base + '.sys.foreign_keys fk WITH (NOWAIT)
            JOIN ' + @base + '.sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
            WHERE fk.parent_object_id = ' + CAST(@objectId AS NVARCHAR(15)) + '
            FOR XML PATH(N''''), TYPE).value(''.'', ''NVARCHAR(MAX)'')), '''')
            ,'''')
        '
       SET @SQLEXEC4 =    
        '+ IIF('+ @isIndex + ' = 1, ISNULL((
            (
             SELECT
                CHAR(13) + ''CREATE'' + CASE WHEN i.is_unique = 1 THEN '' UNIQUE'' ELSE '''' END 
                + '' NONCLUSTERED INDEX '' + ''' + @bracketStart + ''' + i.name + ''' + @bracketEnd +''' +'' ON '' + ''' + @objectName + ''' + CHAR(13) + ''('' + CHAR(13) + ''' + @tab + ''' + '' '' +
                STUFF((
                SELECT CHAR(13) + ''' + @tab + ''' + '','' + ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd +''' + SPACE(maxLenIdxColumn - lenIdxColumn+1) + CASE WHEN c.is_descending_key = 1 THEN ''DESC'' ELSE ''ASC'' END
                FROM (
                    SELECT 
                         c.name
                        ,c.is_descending_key
                        ,MAX(LEN( ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd +''')) OVER() AS maxLenIdxColumn
                        ,LEN( ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd +''') AS lenIdxColumn
                    FROM index_column c
                    WHERE c.is_included_column = 0
                    AND c.index_id = i.index_id
                ) c
                FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+2, '''') + CHAR(13) + '')'' 
                + ISNULL(CHAR(13) + ''INCLUDE ''+ CHAR(13) + ''('' + CHAR(13) + ''' + @tab + ''' + '' '' + 
                    STUFF((
                    SELECT CHAR(13) + ''' + @tab + ''' + '','' + ''' + @bracketStart + ''' + c.name + ''' + @bracketEnd +'''
                    FROM index_column c
                    WHERE c.is_included_column = 1
                        AND c.index_id = i.index_id
                    FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+2, '''') + CHAR(13) + '')'', '''')  + CHAR(13)
            FROM ' + @base + '.sys.indexes i WITH (NOWAIT)
            WHERE i.[object_id] = ' + CAST(@objectId AS NVARCHAR(15)) + '
                AND i.is_primary_key = 0
                AND i.[type] = 2
            FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)'')
        ), '''')
        ,'''')'

    SET @SQLEXEC5 = '+IIF('+ @isInsert + ' = 1, ISNULL(CHAR(13) + ''INSERT INTO '' + ''' + @objectName + ''' + CHAR(13) + ''('' + CHAR(13) + STUFF((
        SELECT ''' + @tab + ''' + '','' + ''' +  @bracketStart +'''+ c.name + ''' + @bracketEnd +''' + SPACE(mlc.maxLenColumn - LEN(c.name)+1) +
            + CHAR(13)
        FROM ' + @base + '.sys.columns c WITH (NOWAIT)
        JOIN ' + @base + '.sys.types tp WITH (NOWAIT) 
            ON c.user_type_id = tp.user_type_id
        LEFT JOIN ' + @base + '.sys.computed_columns cc WITH (NOWAIT) 
            ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
        LEFT JOIN ' + @base + '.sys.default_constraints dc WITH (NOWAIT) 
            ON c.default_object_id != 0 
            AND c.[object_id] = dc.parent_object_id 
            AND c.column_id = dc.parent_column_id
        LEFT JOIN ' + @base + '.sys.identity_columns ic WITH (NOWAIT) 
            ON c.is_identity = 1 
            AND c.[object_id] = ic.[object_id] 
            AND c.column_id = ic.column_id
        JOIN maxLen mlc ON c.column_id = mlc.column_id        
        WHERE c.[object_id] =  ' + CAST(@objectId AS NVARCHAR(15)) + '
        ORDER BY c.column_id
        FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''), 1, LEN(REPLACE(''' + @tab + ''','' '', ''*''))+1, ''' + @tab + ''' + '' '')   
        + '')'' + CHAR(13) ,'''')
        ,'''')
        PRINT  @SQL
        '
    
    IF(@isPrint = N'0')
        BEGIN
            EXEC (@SQLEXEC + @SQLEXEC2 + @SQLEXEC3 + @SQLEXEC4 + @SQLEXEC5)
        END
    ELSE
        BEGIN
            PRINT @SQLEXEC
            PRINT @SQLEXEC2
            PRINT @SQLEXEC3
            PRINT @SQLEXEC4
            PRINT @SQLEXEC5
        END
END
