USE [LyftDB_Gold]
GO

CREATE VIEW FactTrips AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/gold/fact_tables/FactTrips',
    FORMAT = 'DELTA'
) AS [result];
GO

CREATE VIEW DimDate AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/gold/dimensions/DimDate/',
    FORMAT = 'DELTA'
) AS [result];
GO

CREATE VIEW DimPickupLocation AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/gold/dimensions/DimPickupLocation/',
    FORMAT = 'DELTA'
) AS [result];
GO

CREATE VIEW DimDropoffLocation AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/gold/dimensions/DimDropoffLocation/',
    FORMAT = 'DELTA'
) AS [result];
GO

CREATE VIEW DimRateCode AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/gold/dimensions/DimRateCode/',
    FORMAT = 'DELTA'
) AS [result];
GO

CREATE VIEW DimPaymentType AS
SELECT * 
FROM OPENROWSET(
    BULK 'https://stlyftdevweu.dfs.core.windows.net/gold/dimensions/DimPaymentType/',
    FORMAT = 'DELTA'
) AS [result];