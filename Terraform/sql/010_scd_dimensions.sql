-- ============================================================
-- E6 - EVOLUTION ENTREPOT : Variations de Dimensions (SCD)
-- Projet Data Engineering - Region Hauts-de-France
-- Implementation des SCD Type 1, 2 et 3 (Ralph Kimball)
-- ============================================================
--
-- Rappel des choix de conception :
--   dim_activite   -> SCD Type 1 (ecrasement, corrections)
--   dim_geographie -> SCD Type 2 (historisation complete)
--   dim_demographie -> SCD Type 3 (colonne precedente)
--
-- Ce script :
--   1. Ajoute les colonnes SCD aux dimensions concernees
--   2. Cree les procedures stockees pour chaque type de SCD
--   3. Cree une vue analytique pour consulter l'historique
-- ============================================================


-- ============================================================
-- PARTIE 1 : SCD TYPE 1 - dim_activite (Ecrasement)
-- ============================================================
-- Le Type 1 n'ajoute pas de colonnes : on ecrase simplement
-- l'ancienne valeur. L'ancien libelle est trace dans log_etl.
-- ============================================================

IF OBJECT_ID('dwh.sp_scd_type1_activite', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_scd_type1_activite;
GO

CREATE PROCEDURE dwh.sp_scd_type1_activite
    @naf_section_code    NVARCHAR(5),
    @nouveau_libelle     NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ancien_libelle NVARCHAR(255)
    DECLARE @message NVARCHAR(500)

    -- Recuperer l'ancien libelle pour le tracer dans les logs
    SELECT @ancien_libelle = naf_section_libelle
    FROM dwh.dim_activite
    WHERE naf_section_code = @naf_section_code;

    -- Verifier que le code existe
    IF @ancien_libelle IS NULL
    BEGIN
        SET @message = 'SCD Type 1 : code NAF "' + @naf_section_code + '" introuvable'
        EXEC dwh.sp_log_etl
            @etape = 'SCD_TYPE1',
            @table_cible = 'dim_activite',
            @statut = 'WARNING',
            @message = @message
        PRINT @message
        RETURN
    END

    -- Verifier que la valeur a reellement change
    IF @ancien_libelle = @nouveau_libelle
    BEGIN
        PRINT 'SCD Type 1 : aucun changement detecte pour ' + @naf_section_code
        RETURN
    END

    -- Journaliser l'ancienne valeur (audit)
    SET @message = 'Ancien libelle: "' + @ancien_libelle
        + '" -> Nouveau: "' + @nouveau_libelle + '"'

    EXEC dwh.sp_log_etl
        @etape = 'SCD_TYPE1',
        @table_cible = 'dim_activite',
        @statut = 'INFO',
        @message = @message

    -- Ecrasement de la valeur (Type 1)
    UPDATE dwh.dim_activite
    SET naf_section_libelle = @nouveau_libelle,
        date_modification = GETDATE()
    WHERE naf_section_code = @naf_section_code;

    -- Journaliser le succes
    EXEC dwh.sp_log_etl
        @etape = 'SCD_TYPE1',
        @table_cible = 'dim_activite',
        @statut = 'SUCCES',
        @nb_lignes = 1,
        @message = @message

    PRINT 'SCD Type 1 applique sur dim_activite : ' + @naf_section_code
END;
GO

PRINT 'Procedure dwh.sp_scd_type1_activite creee';
GO


-- ============================================================
-- PARTIE 2 : SCD TYPE 2 - dim_geographie (Historisation)
-- ============================================================
-- Le Type 2 ajoute 4 colonnes pour gerer les versions :
--   date_debut_validite  : quand cette version est devenue active
--   date_fin_validite    : quand cette version a ete fermee (NULL = active)
--   est_actif            : 1 = version courante, 0 = historique
--   version              : numero de version incremental
-- ============================================================

-- Ajout des colonnes SCD Type 2 a dim_geographie (si absentes)
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dwh.dim_geographie')
      AND name = 'date_debut_validite'
)
BEGIN
    ALTER TABLE dwh.dim_geographie ADD
        date_debut_validite DATETIME DEFAULT '2010-01-01';
    PRINT 'Colonne date_debut_validite ajoutee a dim_geographie';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dwh.dim_geographie')
      AND name = 'date_fin_validite'
)
BEGIN
    ALTER TABLE dwh.dim_geographie ADD
        date_fin_validite DATETIME NULL;
    PRINT 'Colonne date_fin_validite ajoutee a dim_geographie';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dwh.dim_geographie')
      AND name = 'est_actif'
)
BEGIN
    ALTER TABLE dwh.dim_geographie ADD
        est_actif BIT DEFAULT 1;
    PRINT 'Colonne est_actif ajoutee a dim_geographie';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dwh.dim_geographie')
      AND name = 'version'
)
BEGIN
    ALTER TABLE dwh.dim_geographie ADD
        version INT DEFAULT 1;
    PRINT 'Colonne version ajoutee a dim_geographie';
END
GO

-- Index sur est_actif pour les requetes courantes (WHERE est_actif = 1)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_dim_geo_actif')
BEGIN
    CREATE NONCLUSTERED INDEX IX_dim_geo_actif
    ON dwh.dim_geographie (est_actif)
    INCLUDE (commune_code, commune_nom, departement_code);
    PRINT 'Index IX_dim_geo_actif cree';
END
GO

-- Initialiser les enregistrements existants (tous actifs, version 1)
UPDATE dwh.dim_geographie
SET date_debut_validite = ISNULL(date_debut_validite, '2010-01-01'),
    est_actif = ISNULL(est_actif, 1),
    version = ISNULL(version, 1)
WHERE est_actif IS NULL OR version IS NULL;
GO

-- Procedure SCD Type 2
IF OBJECT_ID('dwh.sp_scd_type2_geographie', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_scd_type2_geographie;
GO

CREATE PROCEDURE dwh.sp_scd_type2_geographie
    @commune_code     NVARCHAR(10),
    @nouveau_nom      NVARCHAR(255),
    @nouveau_code     NVARCHAR(10) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ancienne_version INT
    DECLARE @ancien_id INT
    DECLARE @ancien_nom NVARCHAR(255)
    DECLARE @message NVARCHAR(500)

    -- Recuperer la version courante (active)
    SELECT
        @ancienne_version = version,
        @ancien_id = geo_id,
        @ancien_nom = commune_nom
    FROM dwh.dim_geographie
    WHERE commune_code = @commune_code AND est_actif = 1;

    -- Verifier que le code commune existe
    IF @ancien_id IS NULL
    BEGIN
        SET @message = 'SCD Type 2 : commune "' + @commune_code + '" introuvable (pas de version active)'
        EXEC dwh.sp_log_etl
            @etape = 'SCD_TYPE2',
            @table_cible = 'dim_geographie',
            @statut = 'WARNING',
            @message = @message
        PRINT @message
        RETURN
    END

    -- Verifier que la valeur a reellement change
    IF @ancien_nom = @nouveau_nom AND (@nouveau_code IS NULL OR @nouveau_code = @commune_code)
    BEGIN
        PRINT 'SCD Type 2 : aucun changement detecte pour commune ' + @commune_code
        RETURN
    END

    -- ETAPE 1 : Fermer l'ancien enregistrement
    UPDATE dwh.dim_geographie
    SET date_fin_validite = GETDATE(),
        est_actif = 0,
        date_modification = GETDATE()
    WHERE geo_id = @ancien_id;

    -- ETAPE 2 : Inserer la nouvelle version
    INSERT INTO dwh.dim_geographie (
        commune_code, commune_nom, codes_postaux,
        departement_code, departement_nom,
        region_code, region_nom,
        longitude, latitude, surface_km2, population_reference,
        niveau_geo, geo_code_source,
        date_debut_validite, date_fin_validite,
        est_actif, version,
        date_creation, date_modification
    )
    SELECT
        ISNULL(@nouveau_code, commune_code),
        @nouveau_nom,
        codes_postaux,
        departement_code, departement_nom,
        region_code, region_nom,
        longitude, latitude, surface_km2, population_reference,
        niveau_geo, geo_code_source,
        GETDATE(),       -- date_debut_validite = maintenant
        NULL,            -- date_fin_validite = NULL (version active)
        1,               -- est_actif = 1
        @ancienne_version + 1,
        GETDATE(), GETDATE()
    FROM dwh.dim_geographie
    WHERE geo_id = @ancien_id;

    -- Journaliser le changement
    SET @message = 'Nouvelle version (v' + CAST(@ancienne_version + 1 AS VARCHAR)
        + ') pour commune ' + @commune_code
        + ' : "' + ISNULL(@ancien_nom, '') + '" -> "' + @nouveau_nom + '"'

    EXEC dwh.sp_log_etl
        @etape = 'SCD_TYPE2',
        @table_cible = 'dim_geographie',
        @statut = 'SUCCES',
        @nb_lignes = 1,
        @message = @message

    PRINT @message
END;
GO

PRINT 'Procedure dwh.sp_scd_type2_geographie creee';
GO


-- ============================================================
-- PARTIE 3 : SCD TYPE 3 - dim_demographie (Colonne precedente)
-- ============================================================
-- Le Type 3 ajoute 2 colonnes pour conserver l'ancienne valeur :
--   ancien_pcs_libelle    : le libelle PCS avant le changement
--   date_changement_pcs   : la date du dernier changement
-- ============================================================

-- Ajout des colonnes SCD Type 3 a dim_demographie (si absentes)
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dwh.dim_demographie')
      AND name = 'ancien_pcs_libelle'
)
BEGIN
    ALTER TABLE dwh.dim_demographie ADD
        ancien_pcs_libelle NVARCHAR(255) NULL;
    PRINT 'Colonne ancien_pcs_libelle ajoutee a dim_demographie';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dwh.dim_demographie')
      AND name = 'date_changement_pcs'
)
BEGIN
    ALTER TABLE dwh.dim_demographie ADD
        date_changement_pcs DATETIME NULL;
    PRINT 'Colonne date_changement_pcs ajoutee a dim_demographie';
END
GO

-- Procedure SCD Type 3
IF OBJECT_ID('dwh.sp_scd_type3_demographie', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_scd_type3_demographie;
GO

CREATE PROCEDURE dwh.sp_scd_type3_demographie
    @pcs_code        NVARCHAR(5),
    @nouveau_libelle NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ancien_libelle NVARCHAR(255)
    DECLARE @nb_modif INT
    DECLARE @message NVARCHAR(500)

    -- Recuperer l'ancien libelle
    SELECT TOP 1 @ancien_libelle = pcs_libelle
    FROM dwh.dim_demographie
    WHERE pcs_code = @pcs_code;

    -- Verifier que le code PCS existe
    IF @ancien_libelle IS NULL
    BEGIN
        SET @message = 'SCD Type 3 : code PCS "' + @pcs_code + '" introuvable'
        EXEC dwh.sp_log_etl
            @etape = 'SCD_TYPE3',
            @table_cible = 'dim_demographie',
            @statut = 'WARNING',
            @message = @message
        PRINT @message
        RETURN
    END

    -- Sauvegarder l'ancien libelle et mettre a jour
    -- Ne mettre a jour que si le libelle a reellement change
    UPDATE dwh.dim_demographie
    SET ancien_pcs_libelle = pcs_libelle,
        pcs_libelle = @nouveau_libelle,
        date_changement_pcs = GETDATE(),
        date_modification = GETDATE()
    WHERE pcs_code = @pcs_code
      AND pcs_libelle <> @nouveau_libelle;

    SET @nb_modif = @@ROWCOUNT

    IF @nb_modif = 0
    BEGIN
        PRINT 'SCD Type 3 : aucun changement detecte pour PCS ' + @pcs_code
        RETURN
    END

    -- Journaliser le changement
    SET @message = 'PCS ' + @pcs_code
        + ' : "' + ISNULL(@ancien_libelle, '') + '" -> "' + @nouveau_libelle + '"'
        + ' (' + CAST(@nb_modif AS VARCHAR) + ' lignes)'

    EXEC dwh.sp_log_etl
        @etape = 'SCD_TYPE3',
        @table_cible = 'dim_demographie',
        @statut = 'SUCCES',
        @nb_lignes = @nb_modif,
        @message = @message

    PRINT 'SCD Type 3 applique sur dim_demographie : ' + @message
END;
GO

PRINT 'Procedure dwh.sp_scd_type3_demographie creee';
GO


-- ============================================================
-- PARTIE 4 : PROCEDURE MERGE GENERIQUE POUR L'ETL
-- ============================================================
-- Cette procedure applique le SCD Type 2 en mode batch
-- via un MERGE depuis une table staging vers dim_geographie.
-- Utilisee par l'ETL Python lors du chargement des dimensions.
-- ============================================================

IF OBJECT_ID('dwh.sp_merge_dim_geographie', 'P') IS NOT NULL
    DROP PROCEDURE dwh.sp_merge_dim_geographie;
GO

CREATE PROCEDURE dwh.sp_merge_dim_geographie
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @nb_insert INT = 0
    DECLARE @nb_update INT = 0
    DECLARE @nb_close INT = 0
    DECLARE @message NVARCHAR(500)
    DECLARE @date_debut DATETIME = GETDATE()

    EXEC dwh.sp_log_etl
        @etape = 'MERGE_SCD2',
        @table_cible = 'dim_geographie',
        @statut = 'DEBUT',
        @message = 'Demarrage du MERGE SCD Type 2'

    BEGIN TRY
        -- Fermer les enregistrements qui ont change
        UPDATE g
        SET g.date_fin_validite = GETDATE(),
            g.est_actif = 0,
            g.date_modification = GETDATE()
        FROM dwh.dim_geographie g
        INNER JOIN stg.stg_geographie s
            ON g.commune_code = s.commune_code
            AND g.departement_code = s.departement_code
        WHERE g.est_actif = 1
          AND (g.commune_nom <> s.commune_nom
               OR ISNULL(g.population_reference, 0) <> ISNULL(s.population_reference, 0));

        SET @nb_close = @@ROWCOUNT

        -- Inserer les nouvelles versions des enregistrements modifies
        INSERT INTO dwh.dim_geographie (
            commune_code, commune_nom, codes_postaux,
            departement_code, departement_nom,
            region_code, region_nom,
            longitude, latitude, surface_km2, population_reference,
            niveau_geo, geo_code_source,
            date_debut_validite, date_fin_validite,
            est_actif, version,
            date_creation, date_modification
        )
        SELECT
            s.commune_code, s.commune_nom, s.codes_postaux,
            s.departement_code, s.departement_nom,
            s.region_code, s.region_nom,
            s.longitude, s.latitude, s.surface_km2, s.population_reference,
            s.niveau_geo, s.geo_code_source,
            GETDATE(), NULL, 1,
            ISNULL(g.version, 0) + 1,
            GETDATE(), GETDATE()
        FROM stg.stg_geographie s
        LEFT JOIN dwh.dim_geographie g
            ON s.commune_code = g.commune_code
            AND s.departement_code = g.departement_code
            AND g.est_actif = 0
            AND g.date_fin_validite >= DATEADD(MINUTE, -5, GETDATE())
        WHERE EXISTS (
            -- Enregistrements qu'on vient de fermer
            SELECT 1 FROM dwh.dim_geographie closed
            WHERE closed.commune_code = s.commune_code
              AND closed.departement_code = s.departement_code
              AND closed.est_actif = 0
              AND closed.date_fin_validite >= DATEADD(MINUTE, -5, GETDATE())
        );

        SET @nb_update = @@ROWCOUNT

        -- Inserer les nouveaux enregistrements (qui n'existaient pas du tout)
        INSERT INTO dwh.dim_geographie (
            commune_code, commune_nom, codes_postaux,
            departement_code, departement_nom,
            region_code, region_nom,
            longitude, latitude, surface_km2, population_reference,
            niveau_geo, geo_code_source,
            date_debut_validite, date_fin_validite,
            est_actif, version,
            date_creation, date_modification
        )
        SELECT
            s.commune_code, s.commune_nom, s.codes_postaux,
            s.departement_code, s.departement_nom,
            s.region_code, s.region_nom,
            s.longitude, s.latitude, s.surface_km2, s.population_reference,
            s.niveau_geo, s.geo_code_source,
            GETDATE(), NULL, 1, 1,
            GETDATE(), GETDATE()
        FROM stg.stg_geographie s
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.dim_geographie g
            WHERE g.commune_code = s.commune_code
              AND g.departement_code = s.departement_code
        );

        SET @nb_insert = @@ROWCOUNT

        -- Journaliser le resultat
        SET @message = 'MERGE SCD2 termine : '
            + CAST(@nb_close AS VARCHAR) + ' fermes, '
            + CAST(@nb_update AS VARCHAR) + ' nouvelles versions, '
            + CAST(@nb_insert AS VARCHAR) + ' nouveaux'

        EXEC dwh.sp_log_etl
            @etape = 'MERGE_SCD2',
            @table_cible = 'dim_geographie',
            @statut = 'SUCCES',
            @nb_lignes = @nb_update,
            @duree_sec = 0,
            @message = @message

        PRINT @message
    END TRY
    BEGIN CATCH
        SET @message = 'Echec MERGE SCD2 : ' + ERROR_MESSAGE()

        EXEC dwh.sp_log_etl
            @etape = 'MERGE_SCD2',
            @table_cible = 'dim_geographie',
            @statut = 'ERREUR',
            @message = @message

        EXEC dwh.sp_log_erreur
            @source = 'sp_merge_dim_geographie',
            @type_erreur = 'SQL',
            @message = @message

        PRINT @message
    END CATCH
END;
GO

PRINT 'Procedure dwh.sp_merge_dim_geographie creee';
GO


-- ============================================================
-- PARTIE 5 : VUES ANALYTIQUES SCD
-- ============================================================

-- Vue : historique complet des changements geographiques
IF OBJECT_ID('analytics.v_historique_geographie', 'V') IS NOT NULL
    DROP VIEW analytics.v_historique_geographie;
GO

CREATE VIEW analytics.v_historique_geographie AS
SELECT
    geo_id,
    commune_code,
    commune_nom,
    departement_code,
    departement_nom,
    date_debut_validite,
    date_fin_validite,
    est_actif,
    version,
    CASE
        WHEN est_actif = 1 THEN 'Version courante'
        ELSE 'Version historique (v' + CAST(version AS VARCHAR) + ')'
    END AS statut_version
FROM dwh.dim_geographie;
GO

PRINT 'Vue analytics.v_historique_geographie creee';
GO

-- Vue : changements PCS (Type 3)
IF OBJECT_ID('analytics.v_changements_pcs', 'V') IS NOT NULL
    DROP VIEW analytics.v_changements_pcs;
GO

CREATE VIEW analytics.v_changements_pcs AS
SELECT
    demo_id,
    pcs_code,
    pcs_libelle AS libelle_actuel,
    ancien_pcs_libelle AS libelle_precedent,
    date_changement_pcs,
    CASE
        WHEN ancien_pcs_libelle IS NOT NULL THEN 'Modifie'
        ELSE 'Original'
    END AS statut_changement
FROM dwh.dim_demographie
WHERE pcs_code IS NOT NULL;
GO

PRINT 'Vue analytics.v_changements_pcs creee';
GO

-- Vue : resume de tous les changements SCD (tableau de bord)
IF OBJECT_ID('analytics.v_resume_scd', 'V') IS NOT NULL
    DROP VIEW analytics.v_resume_scd;
GO

CREATE VIEW analytics.v_resume_scd AS
SELECT
    etape AS type_scd,
    table_cible AS dimension,
    COUNT(*) AS nb_operations,
    SUM(CASE WHEN statut = 'SUCCES' THEN 1 ELSE 0 END) AS nb_succes,
    SUM(CASE WHEN statut = 'ERREUR' THEN 1 ELSE 0 END) AS nb_erreurs,
    MAX(date_execution) AS derniere_execution
FROM dwh.log_etl
WHERE etape LIKE 'SCD_%' OR etape = 'MERGE_SCD2'
GROUP BY etape, table_cible;
GO

PRINT 'Vue analytics.v_resume_scd creee';
GO


-- ============================================================
-- PERMISSIONS
-- ============================================================

-- Le role ETL peut executer les procedures SCD et MERGE
GRANT EXECUTE ON dwh.sp_scd_type1_activite TO role_etl_process;
GRANT EXECUTE ON dwh.sp_scd_type2_geographie TO role_etl_process;
GRANT EXECUTE ON dwh.sp_scd_type3_demographie TO role_etl_process;
GRANT EXECUTE ON dwh.sp_merge_dim_geographie TO role_etl_process;

-- Les admins aussi
GRANT EXECUTE ON dwh.sp_scd_type1_activite TO role_dwh_admin;
GRANT EXECUTE ON dwh.sp_scd_type2_geographie TO role_dwh_admin;
GRANT EXECUTE ON dwh.sp_scd_type3_demographie TO role_dwh_admin;
GRANT EXECUTE ON dwh.sp_merge_dim_geographie TO role_dwh_admin;

-- Les analystes peuvent consulter les vues
GRANT SELECT ON analytics.v_historique_geographie TO role_analyst;
GRANT SELECT ON analytics.v_changements_pcs TO role_analyst;
GRANT SELECT ON analytics.v_resume_scd TO role_analyst;
GRANT SELECT ON analytics.v_historique_geographie TO role_bi_reader;
GRANT SELECT ON analytics.v_changements_pcs TO role_bi_reader;
GRANT SELECT ON analytics.v_resume_scd TO role_bi_reader;

PRINT '========================================';
PRINT 'CONFIGURATION SCD DIMENSIONS TERMINEE';
PRINT '========================================';
