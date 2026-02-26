-- ============================================================
-- E6 - EVOLUTION ENTREPOT : Configuration Backup (Azure SQL)
-- Projet Data Engineering - Region Hauts-de-France
-- Vues de monitoring des sauvegardes Azure SQL Database.
--
-- Le backup complet est gere par le script Python
-- analytics/etl/backup_to_datalake.py (export .bacpac vers ADLS Gen2).
-- Utilisation : python run_etl.py --backup
-- ============================================================

-- Nettoyage des anciennes procedures (remplacees par Python)
IF OBJECT_ID('dwh.sp_backup_complet', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_backup_complet;
GO

IF OBJECT_ID('dwh.sp_backup_partiel', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_backup_partiel;
GO

IF OBJECT_ID('dwh.sp_nettoyer_backups', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_nettoyer_backups;
GO

IF OBJECT_ID('analytics.v_backups_disponibles', 'V') IS NOT NULL
    DROP VIEW analytics.v_backups_disponibles;
GO

-- ============================================================
-- VUE : Etat des sauvegardes automatiques Azure
-- Affiche les backups automatiques Azure (PITR)
-- ============================================================

IF OBJECT_ID('analytics.v_etat_backup_azure', 'V') IS NOT NULL
    DROP VIEW analytics.v_etat_backup_azure;
GO

CREATE VIEW analytics.v_etat_backup_azure AS
SELECT
    DB_NAME() AS nom_base,
    b.backup_type AS type_backup,
    CASE b.backup_type
        WHEN 'D' THEN 'Complet (Full)'
        WHEN 'I' THEN 'Differentiel'
        WHEN 'L' THEN 'Journal de transactions'
        ELSE 'Inconnu'
    END AS description_type,
    b.backup_start_date AS dernier_backup,
    b.backup_finish_date AS fin_dernier_backup,
    DATEDIFF(MINUTE, b.backup_start_date, b.backup_finish_date) AS duree_minutes,
    CAST(b.backup_size_in_bytes / 1048576.0 AS DECIMAL(10,2)) AS taille_mo,
    GETDATE() AS date_verification
FROM sys.dm_database_backups b;
GO

PRINT 'Vue analytics.v_etat_backup_azure creee';
GO

-- ============================================================
-- VUE : Historique des backups (basee sur les logs ETL)
-- Inclut les backups BACPAC generes par le script Python
-- ============================================================

IF OBJECT_ID('analytics.v_historique_backups', 'V') IS NOT NULL
    DROP VIEW analytics.v_historique_backups;
GO

CREATE VIEW analytics.v_historique_backups AS
SELECT
    log_id,
    date_execution,
    etape AS type_backup,
    statut,
    message,
    duree_secondes,
    nb_lignes,
    utilisateur
FROM dwh.log_etl
WHERE etape IN ('BACKUP_BACPAC', 'RESTAURATION');
GO

PRINT 'Vue analytics.v_historique_backups creee';
GO

-- ============================================================
-- PERMISSIONS
-- ============================================================

GRANT SELECT ON analytics.v_historique_backups TO role_analyst;
GRANT SELECT ON analytics.v_historique_backups TO role_consultant;
GRANT SELECT ON analytics.v_etat_backup_azure TO role_analyst;
GRANT SELECT ON analytics.v_etat_backup_azure TO role_consultant;

PRINT '========================================';
PRINT 'CONFIGURATION BACKUP AZURE SQL TERMINEE';
PRINT '========================================';
