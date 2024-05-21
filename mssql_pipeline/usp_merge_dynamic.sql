/*
Procedure: dbo.usp_dynamic_merge
Purpose: To dynamically build T-SQL merge statement from z_staging_dynamic to target table.
		 Client must specify target table and which column/columns to merge in z_merge_columns table
		 Ensure staging data is in the same ordinal position as data in the target table.

Examples:
	1 column to merge on		
		exec usp_dynamic_merge 'my_target_table'

*/
CREATE PROCEDURE [dbo].[usp_dynamic_merge]
	@target AS VARCHAR(MAX)
	AS
	SET NOCOUNT ON;

	DECLARE @result1 TABLE(COLUMN_NAME varchar(MAX), ORDINAL_POSITION int, DATA_TYPE varchar(MAX), CHARACTER_MAXIMUM_LENGTH INT)
	DECLARE @result2 TABLE(target_table varchar(MAX), source_col VARCHAR(MAX), target_col varchar(MAX))
	DECLARE @sql AS VARCHAR(MAX)
		
	INSERT INTO @result1
	select COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
	from INFORMATION_SCHEMA.COLUMNS
	where table_name = @target

	INSERT INTO @result2
	select target_table, source_col, target_col
	from z_merge_columns
	where target_table = @target

	SET @sql = 'MERGE ' + @target + ' AS Target'
				+  CHAR(13) +
				'USING z_staging_dynamic AS Source '
				+ CHAR(13) +
				+ 'ON ' +   REPLACE(STUFF((SELECT ', ' +'Source.'+ r2.source_col + ' = ' + 'Target.' + r2.target_col
							FROM @result2 r2
							FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,''),',',' AND')
				+ CHAR(13) +
				+ CHAR(13) +
				+ 'WHEN NOT MATCHED BY Target THEN' +  CHAR(13) +
					'	INSERT (' + STUFF((SELECT ', ' + CHAR(13)  + '		' +  QUOTENAME(r.COLUMN_NAME)
							FROM @result1 r
							FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') + CHAR(13) + '	)'
							+  CHAR(13)  +
					'	VALUES (' + STUFF((SELECT ', ' + CHAR(13) + '		CAST(Source.COL' + CAST(r.ORDINAL_POSITION AS varchar) + ' AS '  + r.DATA_TYPE + CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 AND DATA_TYPE = 'varchar' THEN '(MAX)' ELSE '' END + ')'
							FROM @result1 r
							FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') + CHAR(13) +  '	)'
							+  CHAR(13) +
							+ CHAR(13) +

					'WHEN MATCHED THEN UPDATE SET ' +
					STUFF((SELECT ', ' +  CHAR(13) + '	Target.' + QUOTENAME(r.COLUMN_NAME) + ' = CAST(Source.COL' + CAST(r.ORDINAL_POSITION AS varchar) + ' AS ' +  + r.DATA_TYPE + CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 AND DATA_TYPE = 'varchar' THEN '(MAX)' ELSE '' END  +  ')'
							FROM @result1 r
							FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') + ';'
					+ CHAR(13) +
					+ CHAR(13) +
					'TRUNCATE TABLE z_staging_dynamic;'
					
	exec (@sql)
GO