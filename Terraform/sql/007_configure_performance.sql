-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Configuration Performance
-- Projet Data Engineering - Region Hauts-de-France
-- Optimisations pour requetes analytiques
-- ============================================================

-- ============================================================
-- INDEX COLUMNSTORE POUR LES TABLES DE FAITS
-- Optimisation pour les agregations analytiques
-- ============================================================

-- Index columnstore sur fait_population
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'CCI_fait_population')
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fait_population
    ON dwh.fait_population;
    PRINT 'Index columnstore cree sur fait_population';
END
GO

-- Index columnstore sur fait_evenements_demo
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'CCI_fait_evenements_demo')
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fait_evenements_demo
    ON dwh.fait_evenements_demo;
    PRINT 'Index columnstore cree sur fait_evenements_demo';
END
GO

-- Index columnstore sur fait_entreprises
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'CCI_fait_entreprises')
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fait_entreprises
    ON dwh.fait_entreprises;
    PRINT 'Index columnstore cree sur fait_entreprises';
END
GO

-- Index columnstore sur fait_emploi
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'CCI_fait_emploi')
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fait_emploi
    ON dwh.fait_emploi;
    PRINT 'Index columnstore cree sur fait_emploi';
END
GO

-- Index columnstore sur fait_revenus
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'CCI_fait_revenus')
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fait_revenus
    ON dwh.fait_revenus;
    PRINT 'Index columnstore cree sur fait_revenus';
END
GO

-- Index columnstore sur fait_logement
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'CCI_fait_logement')
BEGIN
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fait_logement
    ON dwh.fait_logement;
    PRINT 'Index columnstore cree sur fait_logement';
END
GO

-- ============================================================
-- STATISTIQUES POUR L'OPTIMISEUR DE REQUETES
-- ============================================================

-- Mise a jour des statistiques sur les dimensions
UPDATE STATISTICS dwh.dim_temps WITH FULLSCAN;
UPDATE STATISTICS dwh.dim_geographie WITH FULLSCAN;
UPDATE STATISTICS dwh.dim_demographie WITH FULLSCAN;
UPDATE STATISTICS dwh.dim_activite WITH FULLSCAN;
UPDATE STATISTICS dwh.dim_indicateur WITH FULLSCAN;
UPDATE STATISTICS dwh.dim_logement WITH FULLSCAN;

PRINT 'Statistiques mises a jour sur les dimensions';
GO

-- ============================================================
-- CONFIGURATION DE LA BASE DE DONNEES
-- ============================================================

-- Activer la compression de page sur les dimensions (economie espace)
ALTER TABLE dwh.dim_geographie REBUILD WITH (DATA_COMPRESSION = PAGE);
ALTER TABLE dwh.dim_demographie REBUILD WITH (DATA_COMPRESSION = PAGE);
ALTER TABLE dwh.dim_activite REBUILD WITH (DATA_COMPRESSION = PAGE);
ALTER TABLE dwh.dim_indicateur REBUILD WITH (DATA_COMPRESSION = PAGE);

PRINT 'Compression de page activee sur les dimensions';
GO

-- ============================================================
-- PROCEDURES DE MAINTENANCE
-- ============================================================

-- Procedure de maintenance des index
IF OBJECT_ID('dwh.sp_maintenance_index', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_maintenance_index;
GO

CREATE PROCEDURE dwh.sp_maintenance_index
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @table_name NVARCHAR(255);
    DECLARE @index_name NVARCHAR(255);
    DECLARE @fragmentation FLOAT;

    -- Curseur sur les index fragmentes
    DECLARE index_cursor CURSOR FOR
    SELECT
        OBJECT_NAME(ips.object_id) AS table_name,
        i.name AS index_name,
        ips.avg_fragmentation_in_percent AS fragmentation
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
      AND i.name IS NOT NULL
      AND OBJECT_SCHEMA_NAME(ips.object_id) IN ('dwh', 'dm');

    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @table_name, @index_name, @fragmentation;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @fragmentation > 30
        BEGIN
            -- Rebuild si fragmentation > 30%
            SET @sql = 'ALTER INDEX ' + QUOTENAME(@index_name) + ' ON ' + QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@table_name))) + '.' + QUOTENAME(@table_name) + ' REBUILD';
            PRINT 'REBUILD: ' + @index_name + ' (fragmentation: ' + CAST(@fragmentation AS VARCHAR) + '%)';
        END
        ELSE
        BEGIN
            -- Reorganize si fragmentation entre 10% et 30%
            SET @sql = 'ALTER INDEX ' + QUOTENAME(@index_name) + ' ON ' + QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@table_name))) + '.' + QUOTENAME(@table_name) + ' REORGANIZE';
            PRINT 'REORGANIZE: ' + @index_name + ' (fragmentation: ' + CAST(@fragmentation AS VARCHAR) + '%)';
        END

        EXEC sp_executesql @sql;
        FETCH NEXT FROM index_cursor INTO @table_name, @index_name, @fragmentation;
    END

    CLOSE index_cursor;
    DEALLOCATE index_cursor;

    PRINT 'Maintenance des index terminee';
END
GO

PRINT 'Procedure sp_maintenance_index creee';
GO

-- ============================================================
-- PROCEDURE DE MISE A JOUR DES STATISTIQUES
-- ============================================================

IF OBJECT_ID('dwh.sp_update_statistics', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_update_statistics;
GO

CREATE PROCEDURE dwh.sp_update_statistics
AS
BEGIN
    SET NOCOUNT ON;

    -- Mise a jour des statistiques sur tous les schemas DWH
    EXEC sp_updatestats;

    PRINT 'Statistiques mises a jour sur toute la base';
END
GO

PRINT 'Procedure sp_update_statistics creee';
GO

-- ============================================================
-- VUE DE MONITORING DES PERFORMANCES
-- ============================================================

IF OBJECT_ID('analytics.v_monitoring_tables', 'V') IS NOT NULL
    DROP VIEW analytics.v_monitoring_tables;
GO

CREATE VIEW analytics.v_monitoring_tables AS
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    p.rows AS row_count,
    CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS DECIMAL(18,2)) AS size_mb,
    CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS DECIMAL(18,2)) AS used_mb,
    MAX(ius.last_user_update) AS last_update,
    MAX(ius.last_user_seek) AS last_read
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT JOIN sys.dm_db_index_usage_stats ius ON t.object_id = ius.object_id AND ius.database_id = DB_ID()
WHERE s.name IN ('stg', 'dwh', 'dm')
  AND t.is_ms_shipped = 0
  AND i.index_id <= 1
GROUP BY s.name, t.name, p.rows;
GO

PRINT 'Vue v_monitoring_tables creee';
GO

PRINT '========================================';
PRINT 'CONFIGURATION PERFORMANCE TERMINEE';
PRINT '========================================';
