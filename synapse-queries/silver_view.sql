USE [LyftDB_Silver]
GO

CREATE VIEW LyftDataSilver AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/silver/',
    FORMAT = 'DELTA'
) AS [result];