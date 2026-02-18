-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Creation des schemas
-- Projet Data Engineering - Region Hauts-de-France
-- ============================================================

-- Schema pour les donnees brutes (staging)
IF NOT EXISTS (SELECT * FROM 
 WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

-- Schema pour l'entrepot de donnees (dimensions et faits)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dwh')
    EXEC('CREATE SCHEMA dwh');
GO

-- Schema pour les datamarts
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dm')
    EXEC('CREATE SCHEMA dm');
GO

-- Schema pour les vues analytiques
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'analytics')
    EXEC('CREATE SCHEMA analytics');
GO

PRINT 'Schemas crees avec succes: stg, dwh, dm, analytics';
