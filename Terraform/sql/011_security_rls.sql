-- ============================================================
-- E6 - ENTREPOT DE DONNEES : Row-Level Security (RLS)
-- Projet Data Engineering - Region Hauts-de-France
-- ============================================================
-- Strategie RLS :
--   Predicate applique sur dwh.dim_geographie (departement_code)
--   -> se propage automatiquement a toutes les vues dm.* et analytics.*
--      car elles jointurent toutes sur dim_geographie via geo_id
--
-- Acces par role :
--   role_admin / role_analyst / role_etl_process : voient TOUT
--   role_consultant : filtre par departement(s) autorises
--
-- Hierarchie employes :
--   DIRECTEUR_REGIONAL    (1)  -> departement_code IS NULL (region entiere)
--   DIRECTEUR_DEPARTEMENT (5)  -> leur seul departement
--   DIRECTEUR_AGENCE      (101)-> departement de leur agence
--   COLLABORATEUR        (~400)-> departement de leur agence
--
-- Departements Hauts-de-France : 02, 59, 60, 62, 80
-- Agences : villes > 10 000 hab (101 communes)
-- ============================================================

-- ============================================================
-- 1. SCHEMA SECURITY
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'security')
    EXEC('CREATE SCHEMA security');
GO

PRINT 'Schema security cree';
GO

-- ============================================================
-- 2. TABLE security.agences
-- 101 agences (villes > 10 000 habitants dans la region)
-- ============================================================

IF OBJECT_ID('security.agences', 'U') IS NOT NULL
    DROP TABLE security.agences;
GO

CREATE TABLE security.agences (
    agence_id           INT             IDENTITY(1,1) PRIMARY KEY,
    commune_code        NVARCHAR(10)    NOT NULL,
    ville               NVARCHAR(100)   NOT NULL,
    departement_code    NVARCHAR(3)     NOT NULL,
    departement_nom     NVARCHAR(100)   NULL,
    region              NVARCHAR(50)    NOT NULL DEFAULT 'Hauts-de-France',
    population          INT             NULL,
    taille_agence       NVARCHAR(20)    NOT NULL,  -- 'GRANDE', 'MOYENNE', 'PETITE'
    nb_collaborateurs   INT             NOT NULL DEFAULT 3,

    date_creation       DATETIME2       DEFAULT GETDATE(),
    date_modification   DATETIME2       DEFAULT GETDATE(),

    CONSTRAINT CK_agences_taille CHECK (taille_agence IN ('GRANDE', 'MOYENNE', 'PETITE')),
    CONSTRAINT UK_agences_commune UNIQUE (commune_code)
);
GO

-- Taille agence selon population :
--   GRANDE  : population > 50 000  -> 6 collaborateurs
--   MOYENNE : population > 15 000  -> 4-5 collaborateurs
--   PETITE  : population >= 10 000 -> 3 collaborateurs

CREATE INDEX IX_agences_dept ON security.agences(departement_code);
CREATE INDEX IX_agences_taille ON security.agences(taille_agence);
GO

PRINT 'Table security.agences creee';
GO

-- ============================================================
-- 3. TABLE security.employes
-- Employes de la structure regionale (directeurs + collaborateurs)
-- ============================================================

IF OBJECT_ID('security.employes', 'U') IS NOT NULL
    DROP TABLE security.employes;
GO

CREATE TABLE security.employes (
    employe_id              INT             IDENTITY(1,1) PRIMARY KEY,
    nom                     NVARCHAR(50)    NOT NULL,
    prenom                  NVARCHAR(50)    NOT NULL,
    login_sql               NVARCHAR(100)   NOT NULL,  -- correspond au USER SQL (prenom.nom)
    email                   NVARCHAR(150)   NULL,
    poste                   NVARCHAR(100)   NULL,

    -- Positionnement dans la hierarchie
    niveau_hierarchique     NVARCHAR(30)    NOT NULL,  -- voir CONSTRAINT ci-dessous
    agence_id               INT             NULL REFERENCES security.agences(agence_id),
    departement_code        NVARCHAR(3)     NULL,      -- NULL = acces region entiere

    -- Lien hierarchique
    manager_id              INT             NULL REFERENCES security.employes(employe_id),

    -- Metadata
    date_creation           DATETIME2       DEFAULT GETDATE(),
    date_modification       DATETIME2       DEFAULT GETDATE(),

    CONSTRAINT CK_employes_niveau CHECK (
        niveau_hierarchique IN (
            'DIRECTEUR_REGIONAL',
            'DIRECTEUR_DEPARTEMENT',
            'DIRECTEUR_AGENCE',
            'COLLABORATEUR'
        )
    ),
    CONSTRAINT UK_employes_login UNIQUE (login_sql)
);
GO

CREATE INDEX IX_employes_agence ON security.employes(agence_id);
CREATE INDEX IX_employes_dept ON security.employes(departement_code);
CREATE INDEX IX_employes_niveau ON security.employes(niveau_hierarchique);
GO

PRINT 'Table security.employes creee';
GO

-- ============================================================
-- 4. TABLE security.utilisateurs_zones
-- Mapping login SQL -> departement(s) autorises pour RLS
-- Rempli automatiquement par le script ETL (load_security.py)
-- ============================================================

IF OBJECT_ID('security.utilisateurs_zones', 'U') IS NOT NULL
    DROP TABLE security.utilisateurs_zones;
GO

CREATE TABLE security.utilisateurs_zones (
    id                  INT             IDENTITY(1,1) PRIMARY KEY,
    login_sql           NVARCHAR(100)   NOT NULL,
    departement_code    NVARCHAR(3)     NULL,    -- NULL = acces a toute la region

    CONSTRAINT UK_zones_login_dept UNIQUE (login_sql, departement_code)
);
GO

CREATE INDEX IX_zones_login ON security.utilisateurs_zones(login_sql);
CREATE INDEX IX_zones_dept  ON security.utilisateurs_zones(departement_code);
GO

PRINT 'Table security.utilisateurs_zones creee';
GO

-- ============================================================
-- 5. FONCTION PREDICAT RLS
-- Filtre les lignes de dwh.dim_geographie selon l utilisateur
-- Logique :
--   - Si login_sql ABSENT de utilisateurs_zones -> acces total
--     (role_admin, role_analyst, role_etl_process ne sont pas dans la table)
--   - Si login_sql PRESENT -> acces limite aux depts autorises
--     (role_consultant uniquement)
--   - departement_code IS NULL dans utilisateurs_zones = region entiere
-- ============================================================

IF OBJECT_ID('security.fn_rls_geographie', 'IF') IS NOT NULL
    DROP FUNCTION security.fn_rls_geographie;
GO

CREATE FUNCTION security.fn_rls_geographie(@departement_code NVARCHAR(3))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS acces_autorise
    WHERE
        -- Cas 1 : utilisateur non enregistre dans zones -> acces total
        --         (admins, analysts, etl_process)
        NOT EXISTS (
            SELECT 1
            FROM security.utilisateurs_zones uz
            WHERE uz.login_sql = USER_NAME()
        )
        OR
        -- Cas 2 : utilisateur enregistre -> verifier ses zones autorisees
        EXISTS (
            SELECT 1
            FROM security.utilisateurs_zones uz
            WHERE uz.login_sql = USER_NAME()
              AND (
                  uz.departement_code = @departement_code  -- son dept specifique
                  OR uz.departement_code IS NULL            -- ou acces region entiere
              )
        );
GO

PRINT 'Fonction predicat security.fn_rls_geographie creee';
GO

-- ============================================================
-- 6. POLITIQUE DE SECURITE (SECURITY POLICY)
-- Appliquee sur dwh.dim_geographie
-- -> filtre propage a toutes les vues dm.* et analytics.*
--    qui jointurent sur cette table
-- ============================================================

-- Supprimer la politique existante si elle existe
IF EXISTS (
    SELECT * FROM sys.security_policies
    WHERE name = 'policy_rls_geographie'
      AND schema_id = SCHEMA_ID('security')
)
    DROP SECURITY POLICY security.policy_rls_geographie;
GO

CREATE SECURITY POLICY security.policy_rls_geographie
    ADD FILTER PREDICATE security.fn_rls_geographie(departement_code)
    ON dwh.dim_geographie
    WITH (STATE = ON, SCHEMABINDING = ON);
GO

PRINT 'Politique RLS policy_rls_geographie activee sur dwh.dim_geographie';
GO

-- ============================================================
-- 7. PERMISSIONS SUR LE SCHEMA SECURITY
-- ============================================================

-- role_admin : controle total du schema security
GRANT CONTROL ON SCHEMA::security TO role_admin;
GO

-- role_etl_process : lecture/ecriture pour charger les donnees
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::security TO role_etl_process;
GO

-- role_consultant : PAS d acces direct au schema security
-- (la fonction RLS s execute avec les droits du proprietaire)

PRINT 'Permissions schema security configurees';
GO

-- ============================================================
-- 8. DONNEES D EXEMPLE : Directeur Regional
-- Les 101 agences et ~507 employes sont charges par
-- le script Python : analytics/etl/load_security.py
-- ============================================================

/*
-- Exemple : insertion du directeur regional
INSERT INTO security.employes (nom, prenom, login_sql, email, poste, niveau_hierarchique, departement_code)
VALUES ('MARTIN', 'Sophie', 'sophie.martin', 'sophie.martin@agence-hdf.fr',
        'Directrice Regionale Hauts-de-France', 'DIRECTEUR_REGIONAL', NULL);

-- Exemple : insertion d un directeur departement (Nord - 59)
INSERT INTO security.employes (nom, prenom, login_sql, email, poste, niveau_hierarchique, departement_code, manager_id)
VALUES ('DUPONT', 'Jean', 'jean.dupont', 'jean.dupont@agence-hdf.fr',
        'Directeur Departement Nord', 'DIRECTEUR_DEPARTEMENT', '59', 1);

-- Autoriser le directeur regional a voir toute la region (departement_code IS NULL)
INSERT INTO security.utilisateurs_zones (login_sql, departement_code)
VALUES ('sophie.martin', NULL);

-- Autoriser le directeur departement Nord uniquement
INSERT INTO security.utilisateurs_zones (login_sql, departement_code)
VALUES ('jean.dupont', '59');

-- Creation des utilisateurs SQL correspondants :
CREATE USER [sophie.martin] WITH PASSWORD = 'MotDePasseSecurise!1';
ALTER ROLE role_consultant ADD MEMBER [sophie.martin];

CREATE USER [jean.dupont] WITH PASSWORD = 'MotDePasseSecurise!2';
ALTER ROLE role_consultant ADD MEMBER [jean.dupont];
*/

-- ============================================================
-- 9. VUE DE MONITORING DES ACCES
-- Vue consolidee : employe + role SQL + zone RLS + agence
-- Utile pour auditer qui a acces a quoi
-- ============================================================

IF OBJECT_ID('security.v_acces_employes', 'V') IS NOT NULL
    DROP VIEW security.v_acces_employes;
GO

CREATE VIEW security.v_acces_employes AS
WITH zones_agg AS (
    -- Agregation des zones RLS par login (generalement 1 ligne par employe)
    SELECT
        login_sql,
        STRING_AGG(ISNULL(departement_code, 'REGION_ENTIERE'), ', ') AS zones_autorisees
    FROM security.utilisateurs_zones
    GROUP BY login_sql
),
roles_agg AS (
    -- Roles SQL assignes via sys.database_role_members
    SELECT
        dp_user.name                                    AS login_sql,
        STRING_AGG(dp_role.name, ', ')                  AS roles_sql
    FROM sys.database_role_members drm
    JOIN sys.database_principals dp_user
        ON dp_user.principal_id = drm.member_principal_id
    JOIN sys.database_principals dp_role
        ON dp_role.principal_id = drm.role_principal_id
    WHERE dp_role.type = 'R'   -- R = role de base de donnees
    GROUP BY dp_user.name
)
SELECT
    e.employe_id,
    e.prenom + ' ' + e.nom          AS nom_complet,
    e.login_sql,
    e.email,
    e.poste,
    e.niveau_hierarchique,

    -- Agence de rattachement
    a.ville                         AS agence_ville,
    a.departement_nom               AS agence_departement,
    a.taille_agence,

    -- Role(s) SQL assigne(s) dans la base de donnees
    -- 'Non cree' = employe present dans security.employes
    --              mais pas encore cree en tant qu utilisateur SQL
    ISNULL(r.roles_sql, 'Non cree') AS roles_sql,

    -- Zone(s) de filtrage RLS
    -- 'Acces total (non filtre)' = login absent de utilisateurs_zones
    --   -> concerne role_admin, role_analyst, role_etl_process
    -- 'REGION_ENTIERE' = departement_code IS NULL dans utilisateurs_zones
    --   -> directeur regional (acces tous departements)
    -- '59', '62'... = filtrage par departement
    --   -> directeurs departement, directeurs agence, collaborateurs
    ISNULL(z.zones_autorisees, 'Acces total (non filtre)') AS zones_rls

FROM security.employes e
LEFT JOIN security.agences a    ON a.agence_id  = e.agence_id
LEFT JOIN zones_agg z           ON z.login_sql   = e.login_sql
LEFT JOIN roles_agg r           ON r.login_sql   = e.login_sql;
GO

PRINT 'Vue security.v_acces_employes creee';
GO

-- ============================================================
-- 10. VUE DE MONITORING DES CONNEXIONS ACTIVES
-- Qui est connecte EN CE MOMENT a la base de donnees ?
-- Source : sys.dm_exec_sessions (sessions actives Azure SQL)
-- Necessite : VIEW DATABASE STATE (accorde a role_admin)
-- ============================================================

IF OBJECT_ID('security.v_connexions_actives', 'V') IS NOT NULL
    DROP VIEW security.v_connexions_actives;
GO

CREATE VIEW security.v_connexions_actives AS
SELECT
    s.session_id,
    s.login_name                            AS login_sql,

    -- Identite employe (NULL si login non repertorie dans security.employes)
    e.prenom + ' ' + e.nom                  AS nom_complet,
    e.niveau_hierarchique,
    e.poste,
    a.ville                                 AS agence_ville,
    a.departement_nom                       AS agence_departement,

    -- Informations de session
    s.login_time                            AS heure_connexion,
    s.last_request_start_time               AS derniere_activite,
    DATEDIFF(MINUTE, s.login_time, GETDATE()) AS duree_connexion_min,
    s.status                                AS statut_session,  -- 'running', 'sleeping', 'idle'
    s.host_name                             AS poste_client,
    s.program_name                          AS application,
    s.client_interface_name                 AS interface,

    -- Zone RLS active pour cet utilisateur
    ISNULL(uz_agg.zones_autorisees, 'Acces total (non filtre)') AS zones_rls

FROM sys.dm_exec_sessions s
LEFT JOIN security.employes e
    ON e.login_sql = s.login_name
LEFT JOIN security.agences a
    ON a.agence_id = e.agence_id
LEFT JOIN (
    SELECT login_sql,
           STRING_AGG(ISNULL(departement_code, 'REGION_ENTIERE'), ', ') AS zones_autorisees
    FROM security.utilisateurs_zones
    GROUP BY login_sql
) uz_agg ON uz_agg.login_sql = s.login_name
WHERE s.is_user_process = 1;  -- Exclure les sessions systeme internes
GO

-- Accorder la permission VIEW DATABASE STATE au role_admin
-- (necessaire pour lire sys.dm_exec_sessions)
GRANT VIEW DATABASE STATE TO role_admin;
GO

PRINT 'Vue security.v_connexions_actives creee';
GO

-- ============================================================
-- 11. VERIFICATION DE LA CONFIGURATION RLS
-- ============================================================

PRINT '==========================================';
PRINT 'CONFIGURATION RLS TERMINEE';
PRINT '==========================================';
PRINT 'Schema security cree avec 3 tables :';
PRINT '  security.agences           (101 agences, chargees par ETL)';
PRINT '  security.employes          (~507 employes, charges par ETL)';
PRINT '  security.utilisateurs_zones (mapping RLS login->departement)';
PRINT '';
PRINT 'Predicate RLS : security.fn_rls_geographie';
PRINT 'Politique RLS : security.policy_rls_geographie';
PRINT 'Table ciblee  : dwh.dim_geographie (departement_code)';
PRINT '';
PRINT 'Propagation automatique via JOIN :';
PRINT '  dm.vm_demographie_departement';
PRINT '  dm.vm_entreprises_departement';
PRINT '  dm.vm_revenus_departement';
PRINT '  dm.vm_emploi_departement';
PRINT '  dm.vm_logement_departement';
PRINT '  analytics.v_tableau_bord_territorial';
PRINT '';
PRINT 'Hierarchie :';
PRINT '  DIRECTEUR_REGIONAL    (1)  : region entiere (5 depts)';
PRINT '  DIRECTEUR_DEPARTEMENT (5)  : 1 departement';
PRINT '  DIRECTEUR_AGENCE      (101): dept de leur agence';
PRINT '  COLLABORATEUR        (~400): dept de leur agence';
PRINT '==========================================';
GO
