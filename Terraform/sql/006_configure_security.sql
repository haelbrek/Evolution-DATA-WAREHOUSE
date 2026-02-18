-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Configuration Securite et Acces
-- Projet Data Engineering - Region Hauts-de-France
-- ============================================================

-- ============================================================
-- ROLES DE BASE DE DONNEES
-- ============================================================

-- Role pour les processus ETL (lecture/ecriture staging et dwh)
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_etl_process')
    CREATE ROLE role_etl_process;
GO

-- Role pour les analystes (lecture seule sur datamarts)
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_analyst')
    CREATE ROLE role_analyst;
GO

-- Role pour les administrateurs DWH
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_dwh_admin')
    CREATE ROLE role_dwh_admin;
GO

-- Role pour les applications BI (lecture datamarts + analytics)
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'role_bi_reader')
    CREATE ROLE role_bi_reader;
GO

PRINT 'Roles crees: role_etl_process, role_analyst, role_dwh_admin, role_bi_reader';
GO

-- ============================================================
-- PERMISSIONS ROLE ETL
-- ============================================================

-- Staging : lecture/ecriture complete
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO role_etl_process;
GRANT CREATE TABLE TO role_etl_process;

-- DWH : lecture/ecriture sur dimensions et faits
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dwh TO role_etl_process;

-- Datamarts : lecture seule (les vues sont alimentees par le DWH)
GRANT SELECT ON SCHEMA::dm TO role_etl_process;

-- Analytics : lecture seule
GRANT SELECT ON SCHEMA::analytics TO role_etl_process;

PRINT 'Permissions role_etl_process configurees';
GO

-- ============================================================
-- PERMISSIONS ROLE ANALYST
-- ============================================================

-- Staging : pas d'acces
-- DWH : lecture seule sur dimensions (pour comprendre les codes)
GRANT SELECT ON dwh.dim_temps TO role_analyst;
GRANT SELECT ON dwh.dim_geographie TO role_analyst;
GRANT SELECT ON dwh.dim_demographie TO role_analyst;
GRANT SELECT ON dwh.dim_activite TO role_analyst;
GRANT SELECT ON dwh.dim_indicateur TO role_analyst;
GRANT SELECT ON dwh.dim_logement TO role_analyst;

-- Datamarts : lecture complete
GRANT SELECT ON SCHEMA::dm TO role_analyst;

-- Analytics : lecture complete
GRANT SELECT ON SCHEMA::analytics TO role_analyst;

PRINT 'Permissions role_analyst configurees';
GO

-- ============================================================
-- PERMISSIONS ROLE BI READER
-- ============================================================

-- Acces uniquement aux datamarts et vues analytiques
GRANT SELECT ON SCHEMA::dm TO role_bi_reader;
GRANT SELECT ON SCHEMA::analytics TO role_bi_reader;

-- Acces aux dimensions pour les jointures
GRANT SELECT ON dwh.dim_temps TO role_bi_reader;
GRANT SELECT ON dwh.dim_geographie TO role_bi_reader;

PRINT 'Permissions role_bi_reader configurees';
GO

-- ============================================================
-- PERMISSIONS ROLE ADMIN DWH
-- ============================================================

-- Acces complet a tous les schemas
GRANT CONTROL ON SCHEMA::stg TO role_dwh_admin;
GRANT CONTROL ON SCHEMA::dwh TO role_dwh_admin;
GRANT CONTROL ON SCHEMA::dm TO role_dwh_admin;
GRANT CONTROL ON SCHEMA::analytics TO role_dwh_admin;

-- Permissions de gestion
GRANT ALTER ANY SCHEMA TO role_dwh_admin;
GRANT CREATE TABLE TO role_dwh_admin;
GRANT CREATE VIEW TO role_dwh_admin;
GRANT CREATE PROCEDURE TO role_dwh_admin;

PRINT 'Permissions role_dwh_admin configurees';
GO

-- ============================================================
-- UTILISATEURS DE SERVICE (a creer selon l'environnement)
-- ============================================================

-- Note: Les utilisateurs sont crees via Terraform ou manuellement
-- Exemple de creation d'utilisateur:
/*
-- Utilisateur pour le service ETL
CREATE USER etl_service WITH PASSWORD = 'VotreMotDePasseSecurise123!';
ALTER ROLE role_etl_process ADD MEMBER etl_service;

-- Utilisateur pour Power BI
CREATE USER powerbi_reader WITH PASSWORD = 'VotreMotDePasseSecurise456!';
ALTER ROLE role_bi_reader ADD MEMBER powerbi_reader;

-- Utilisateur analyste
CREATE USER analyst_user WITH PASSWORD = 'VotreMotDePasseSecurise789!';
ALTER ROLE role_analyst ADD MEMBER analyst_user;
*/

PRINT '========================================';
PRINT 'CONFIGURATION SECURITE TERMINEE';
PRINT '========================================';
PRINT 'Roles disponibles:';
PRINT '  - role_etl_process : Pour les jobs ETL';
PRINT '  - role_analyst : Pour les analystes (lecture DM)';
PRINT '  - role_bi_reader : Pour Power BI/Tableau';
PRINT '  - role_dwh_admin : Administration complete';
PRINT '========================================';
