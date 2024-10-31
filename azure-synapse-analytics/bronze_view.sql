USE [LyftDB_Bronze]
GO

CREATE VIEW LyftDataBronze AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/bronze/delta_data/',
    FORMAT = 'DELTA'
) AS [result];