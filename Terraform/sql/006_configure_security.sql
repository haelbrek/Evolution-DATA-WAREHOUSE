-- ============================================================
-- E6 - ENTREPOT DE DONNEES : Configuration Securite et Acces
-- Projet Data Engineering - Region Hauts-de-France
-- ============================================================
-- Roles :
--   role_admin          : Administration complete du DWH
--   role_etl_process    : Pipeline ETL (lecture/ecriture stg + dwh)
--   role_analyst        : Data Analysts internes (lecture complete)
--   role_consultant     : Collaborateurs agence (lecture dm/analytics avec RLS)
-- ============================================================

-- ============================================================
-- 1. CREATION DES ROLES
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_admin')
    CREATE ROLE role_admin;
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_etl_process')
    CREATE ROLE role_etl_process;
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_analyst')
    CREATE ROLE role_analyst;
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_consultant')
    CREATE ROLE role_consultant;
GO

PRINT 'Roles crees : role_admin, role_etl_process, role_analyst, role_consultant';
GO

-- ============================================================
-- 2. PERMISSIONS role_admin
-- ============================================================

GRANT CONTROL ON SCHEMA::stg       TO role_admin;
GRANT CONTROL ON SCHEMA::dwh       TO role_admin;
GRANT CONTROL ON SCHEMA::dm        TO role_admin;
GRANT CONTROL ON SCHEMA::analytics TO role_admin;

GRANT ALTER ANY SCHEMA   TO role_admin;
GRANT CREATE TABLE       TO role_admin;
GRANT CREATE VIEW        TO role_admin;
GRANT CREATE PROCEDURE   TO role_admin;

PRINT 'Permissions role_admin configurees';
GO

-- ============================================================
-- 3. PERMISSIONS role_etl_process
-- ============================================================

-- Staging : lecture / ecriture / suppression + creation de tables
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO role_etl_process;
GRANT CREATE TABLE TO role_etl_process;

-- DWH : lecture / ecriture / suppression
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dwh TO role_etl_process;

-- Datamarts et analytics : lecture seule
GRANT SELECT ON SCHEMA::dm        TO role_etl_process;
GRANT SELECT ON SCHEMA::analytics TO role_etl_process;

PRINT 'Permissions role_etl_process configurees';
GO

-- ============================================================
-- 4. PERMISSIONS role_analyst
-- ============================================================

-- DWH : lecture complete (dimensions + faits)
GRANT SELECT ON SCHEMA::dwh       TO role_analyst;

-- Datamarts et analytics : lecture complete
GRANT SELECT ON SCHEMA::dm        TO role_analyst;
GRANT SELECT ON SCHEMA::analytics TO role_analyst;

-- Pas d acces au staging

PRINT 'Permissions role_analyst configurees';
GO

-- ============================================================
-- 5. PERMISSIONS role_consultant
-- ============================================================
-- Acces uniquement aux datamarts et analytics
-- La restriction geographique est geree par le RLS (voir 011_security_rls.sql)

GRANT SELECT ON SCHEMA::dm        TO role_consultant;
GRANT SELECT ON SCHEMA::analytics TO role_consultant;

-- Pas d acces a stg, dwh, ni au schema security

PRINT 'Permissions role_consultant configurees (RLS actif sur dm et analytics)';
GO

-- ============================================================
-- 6. SUPPRESSION DES ANCIENS ROLES (si existants)
-- ============================================================

IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_bi_reader')
BEGIN
    -- Retirer les membres avant de supprimer
    DECLARE @sql NVARCHAR(500);
    DECLARE @member NVARCHAR(128);
    DECLARE cur CURSOR FOR
        SELECT dp.name FROM sys.database_role_members drm
        JOIN sys.database_principals dp ON drm.member_principal_id = dp.principal_id
        JOIN sys.database_principals rp ON drm.role_principal_id = rp.principal_id
        WHERE rp.name = 'role_bi_reader';
    OPEN cur;
    FETCH NEXT FROM cur INTO @member;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = 'ALTER ROLE role_bi_reader DROP MEMBER [' + @member + ']';
        EXEC sp_executesql @sql;
        FETCH NEXT FROM cur INTO @member;
    END;
    CLOSE cur; DEALLOCATE cur;
    DROP ROLE role_bi_reader;
    PRINT 'Ancien role role_bi_reader supprime';
END
GO

IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_dwh_admin')
BEGIN
    DROP ROLE role_dwh_admin;
    PRINT 'Ancien role role_dwh_admin supprime';
END
GO

-- ============================================================
-- 7. EXEMPLES DE CREATION D UTILISATEURS
-- ============================================================
/*
-- Utilisateur ETL
CREATE USER etl_service WITH PASSWORD = 'MotDePasseSecurise123!';
ALTER ROLE role_etl_process ADD MEMBER etl_service;

-- Data Analyst
CREATE USER data_analyst WITH PASSWORD = 'MotDePasseSecurise456!';
ALTER ROLE role_analyst ADD MEMBER data_analyst;

-- Consultant agence (le login doit correspondre a security.employes.login_sql)
CREATE USER prenom.nom WITH PASSWORD = 'MotDePasseSecurise789!';
ALTER ROLE role_consultant ADD MEMBER prenom.nom;
*/

PRINT '==========================================';
PRINT 'CONFIGURATION SECURITE TERMINEE';
PRINT '==========================================';
PRINT 'Roles actifs :';
PRINT '  role_admin        -> Controle total DWH';
PRINT '  role_etl_process  -> Pipeline ETL';
PRINT '  role_analyst      -> Data Analysts (lecture complete)';
PRINT '  role_consultant   -> Agences (lecture dm/analytics + RLS)';
PRINT '==========================================';
