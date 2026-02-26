-- ============================================================
-- E6 - EVOLUTION ENTREPOT : Configuration Journalisation
-- Projet Data Engineering - Region Hauts-de-France
-- Tables de logging ETL et erreurs + vue de monitoring
-- ============================================================

-- ============================================================
-- TABLE DE JOURNALISATION ETL
-- Enregistre chaque etape du pipeline ETL :
-- debut, succes, erreur, warning avec duree et nb lignes
-- ============================================================

IF OBJECT_ID('dwh.log_etl', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.log_etl (
        log_id          INT IDENTITY(1,1) PRIMARY KEY,
        date_execution  DATETIME DEFAULT GETDATE(),
        etape           NVARCHAR(100),    -- 'load_dimensions', 'load_facts', 'BACKUP', 'SCD_TYPE1'...
        table_cible     NVARCHAR(100),    -- 'dwh.dim_temps', 'dwh.fait_revenus'...
        statut          NVARCHAR(20),     -- 'DEBUT', 'SUCCES', 'ERREUR', 'WARNING'
        nb_lignes       INT DEFAULT 0,
        duree_secondes  FLOAT DEFAULT 0,
        message         NVARCHAR(500),
        utilisateur     NVARCHAR(100) DEFAULT SYSTEM_USER
    );
    PRINT 'Table dwh.log_etl creee';
END
ELSE
    PRINT 'Table dwh.log_etl existe deja';
GO

-- Index sur la date pour les requetes de monitoring
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_log_etl_date')
BEGIN
    CREATE NONCLUSTERED INDEX IX_log_etl_date
    ON dwh.log_etl (date_execution DESC);
    PRINT 'Index IX_log_etl_date cree';
END
GO

-- Index sur le statut pour filtrer les erreurs rapidement
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_log_etl_statut')
BEGIN
    CREATE NONCLUSTERED INDEX IX_log_etl_statut
    ON dwh.log_etl (statut)
    INCLUDE (etape, table_cible, message);
    PRINT 'Index IX_log_etl_statut cree';
END
GO

-- ============================================================
-- TABLE DE JOURNALISATION DES ERREURS
-- Table dediee aux erreurs avec stack trace et suivi
-- de resolution (est_resolu, date_resolution)
-- ============================================================

IF OBJECT_ID('dwh.log_erreurs', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.log_erreurs (
        erreur_id       INT IDENTITY(1,1) PRIMARY KEY,
        date_erreur     DATETIME DEFAULT GETDATE(),
        source          NVARCHAR(100),    -- Script ou procedure source
        type_erreur     NVARCHAR(50),     -- 'SQL', 'ETL', 'CONNEXION', 'DONNEES'
        message_erreur  NVARCHAR(MAX),
        stack_trace     NVARCHAR(MAX),
        est_resolu      BIT DEFAULT 0,
        date_resolution DATETIME NULL
    );
    PRINT 'Table dwh.log_erreurs creee';
END
ELSE
    PRINT 'Table dwh.log_erreurs existe deja';
GO

-- Index sur les erreurs non resolues
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_log_erreurs_non_resolues')
BEGIN
    CREATE NONCLUSTERED INDEX IX_log_erreurs_non_resolues
    ON dwh.log_erreurs (est_resolu, date_erreur DESC)
    WHERE est_resolu = 0;
    PRINT 'Index IX_log_erreurs_non_resolues cree';
END
GO

-- ============================================================
-- PROCEDURE : Enregistrer un log ETL
-- Simplifie l'insertion de logs depuis les procedures SQL
-- ============================================================

IF OBJECT_ID('dwh.sp_log_etl', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_log_etl;
GO

CREATE PROCEDURE dwh.sp_log_etl
    @etape       NVARCHAR(100),
    @table_cible NVARCHAR(100),
    @statut      NVARCHAR(20),
    @nb_lignes   INT = 0,
    @duree_sec   FLOAT = 0,
    @message     NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dwh.log_etl (etape, table_cible, statut, nb_lignes, duree_secondes, message)
    VALUES (@etape, @table_cible, @statut, @nb_lignes, @duree_sec, @message);
END
GO

PRINT 'Procedure dwh.sp_log_etl creee';
GO

-- ============================================================
-- PROCEDURE : Enregistrer une erreur
-- ============================================================

IF OBJECT_ID('dwh.sp_log_erreur', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_log_erreur;
GO

CREATE PROCEDURE dwh.sp_log_erreur
    @source        NVARCHAR(100),
    @type_erreur   NVARCHAR(50),
    @message       NVARCHAR(MAX),
    @stack_trace   NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dwh.log_erreurs (source, type_erreur, message_erreur, stack_trace)
    VALUES (@source, @type_erreur, @message, @stack_trace);
END
GO

PRINT 'Procedure dwh.sp_log_erreur creee';
GO

-- ============================================================
-- PROCEDURE : Marquer une erreur comme resolue
-- ============================================================

IF OBJECT_ID('dwh.sp_resoudre_erreur', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_resoudre_erreur;
GO

CREATE PROCEDURE dwh.sp_resoudre_erreur
    @erreur_id INT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dwh.log_erreurs
    SET est_resolu = 1,
        date_resolution = GETDATE()
    WHERE erreur_id = @erreur_id;
END
GO

PRINT 'Procedure dwh.sp_resoudre_erreur creee';
GO

-- ============================================================
-- VUE DE MONITORING DES ALERTES
-- Agregation quotidienne des evenements ETL pour supervision BI
-- ============================================================

IF OBJECT_ID('analytics.v_monitoring_alertes', 'V') IS NOT NULL
    DROP VIEW analytics.v_monitoring_alertes;
GO

CREATE VIEW analytics.v_monitoring_alertes AS
SELECT
    CAST(date_execution AS DATE) AS jour,
    etape,
    statut,
    COUNT(*) AS nb_evenements,
    SUM(CASE WHEN statut = 'ERREUR' THEN 1 ELSE 0 END) AS nb_erreurs,
    SUM(CASE WHEN statut = 'WARNING' THEN 1 ELSE 0 END) AS nb_warnings,
    AVG(duree_secondes) AS duree_moyenne_sec,
    SUM(nb_lignes) AS total_lignes
FROM dwh.log_etl
GROUP BY CAST(date_execution AS DATE), etape, statut;
GO

PRINT 'Vue analytics.v_monitoring_alertes creee';
GO

-- ============================================================
-- VUE : Erreurs non resolues (pour tableau de bord)
-- ============================================================

IF OBJECT_ID('analytics.v_erreurs_ouvertes', 'V') IS NOT NULL
    DROP VIEW analytics.v_erreurs_ouvertes;
GO

CREATE VIEW analytics.v_erreurs_ouvertes AS
SELECT
    erreur_id,
    date_erreur,
    source,
    type_erreur,
    message_erreur,
    DATEDIFF(HOUR, date_erreur, GETDATE()) AS heures_depuis_erreur
FROM dwh.log_erreurs
WHERE est_resolu = 0;
GO

PRINT 'Vue analytics.v_erreurs_ouvertes creee';
GO

-- ============================================================
-- PERMISSIONS : Donner acces aux roles existants
-- ============================================================

-- Le role ETL peut inserer des logs
GRANT INSERT, SELECT ON dwh.log_etl TO role_etl_process;
GRANT INSERT, SELECT ON dwh.log_erreurs TO role_etl_process;
GRANT EXECUTE ON dwh.sp_log_etl TO role_etl_process;
GRANT EXECUTE ON dwh.sp_log_erreur TO role_etl_process;

-- Les analystes peuvent consulter les vues de monitoring
GRANT SELECT ON analytics.v_monitoring_alertes TO role_analyst;
GRANT SELECT ON analytics.v_erreurs_ouvertes TO role_analyst;
GRANT SELECT ON analytics.v_monitoring_alertes TO role_consultant;
GRANT SELECT ON analytics.v_erreurs_ouvertes TO role_consultant;

-- Les admins peuvent resoudre les erreurs
GRANT EXECUTE ON dwh.sp_resoudre_erreur TO role_admin;

PRINT '========================================';
PRINT 'CONFIGURATION JOURNALISATION TERMINEE';
PRINT '========================================';
