-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Tables de Dimensions
-- Projet Data Engineering - Region Hauts-de-France
-- Schema en etoile (Star Schema)
-- ============================================================

-- ============================================================
-- DIMENSION TEMPS
-- ============================================================
IF OBJECT_ID('dwh.dim_temps', 'U') IS NOT NULL DROP TABLE dwh.dim_temps;
GO

CREATE TABLE dwh.dim_temps (
    temps_id INT IDENTITY(1,1) PRIMARY KEY,
    annee INT NOT NULL,
    trimestre INT NULL,
    mois INT NULL,
    date_complete DATE NULL,
    libelle_periode NVARCHAR(50) NULL,
    est_annee_recensement BIT DEFAULT 0,

    -- Metadata
    date_creation DATETIME2 DEFAULT GETDATE(),
    date_modification DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT UK_dim_temps_annee UNIQUE (annee, trimestre, mois)
);
GO

-- Index pour les recherches frequentes
CREATE INDEX IX_dim_temps_annee ON dwh.dim_temps(annee);
GO

-- ============================================================
-- DIMENSION GEOGRAPHIE
-- ============================================================
IF OBJECT_ID('dwh.dim_geographie', 'U') IS NOT NULL DROP TABLE dwh.dim_geographie;
GO

CREATE TABLE dwh.dim_geographie (
    geo_id INT IDENTITY(1,1) PRIMARY KEY,

    -- Niveau commune
    commune_code NVARCHAR(10) NULL,
    commune_nom NVARCHAR(255) NULL,
    codes_postaux NVARCHAR(MAX) NULL,

    -- Niveau departement
    departement_code NVARCHAR(3) NOT NULL,
    departement_nom NVARCHAR(100) NULL,

    -- Niveau region
    region_code NVARCHAR(3) NULL,
    region_nom NVARCHAR(100) NULL,

    -- Coordonnees geographiques
    longitude FLOAT NULL,
    latitude FLOAT NULL,
    surface_km2 FLOAT NULL,
    population_reference INT NULL,

    -- Hierarchie pour le Data Warehouse
    niveau_geo NVARCHAR(20) NOT NULL DEFAULT 'DEPARTEMENT', -- COMMUNE, DEPARTEMENT, REGION
    geo_code_source NVARCHAR(50) NULL, -- Code original (ex: 2024-DEP-02)

    -- Metadata
    date_creation DATETIME2 DEFAULT GETDATE(),
    date_modification DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT UK_dim_geo_code UNIQUE (departement_code, commune_code)
);
GO

-- Index pour les recherches
CREATE INDEX IX_dim_geo_dept ON dwh.dim_geographie(departement_code);
CREATE INDEX IX_dim_geo_commune ON dwh.dim_geographie(commune_code);
CREATE INDEX IX_dim_geo_niveau ON dwh.dim_geographie(niveau_geo);
GO

-- ============================================================
-- DIMENSION DEMOGRAPHIE
-- ============================================================
IF OBJECT_ID('dwh.dim_demographie', 'U') IS NOT NULL DROP TABLE dwh.dim_demographie;
GO

CREATE TABLE dwh.dim_demographie (
    demo_id INT IDENTITY(1,1) PRIMARY KEY,

    -- Sexe
    sexe_code NVARCHAR(5) NOT NULL,
    sexe_libelle NVARCHAR(50) NULL,

    -- Tranche d'age
    age_code NVARCHAR(20) NULL,
    age_libelle NVARCHAR(100) NULL,
    age_min INT NULL,
    age_max INT NULL,

    -- PCS (Professions et Categories Socioprofessionnelles)
    pcs_code NVARCHAR(5) NULL,
    pcs_libelle NVARCHAR(255) NULL,
    pcs_niveau INT NULL, -- 1=agregat, 2=detail

    -- Metadata
    date_creation DATETIME2 DEFAULT GETDATE(),
    date_modification DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT UK_dim_demo UNIQUE (sexe_code, age_code, pcs_code)
);
GO

CREATE INDEX IX_dim_demo_sexe ON dwh.dim_demographie(sexe_code);
CREATE INDEX IX_dim_demo_pcs ON dwh.dim_demographie(pcs_code);
GO

-- ============================================================
-- DIMENSION ACTIVITE ECONOMIQUE
-- ============================================================
IF OBJECT_ID('dwh.dim_activite', 'U') IS NOT NULL DROP TABLE dwh.dim_activite;
GO

CREATE TABLE dwh.dim_activite (
    activite_id INT IDENTITY(1,1) PRIMARY KEY,

    -- Code NAF (section)
    naf_section_code NVARCHAR(5) NOT NULL,
    naf_section_libelle NVARCHAR(255) NULL,

    -- Forme juridique
    forme_juridique_code NVARCHAR(20) NULL,
    forme_juridique_libelle NVARCHAR(255) NULL,

    -- Regroupements
    secteur_activite NVARCHAR(100) NULL, -- Agriculture, Industrie, Services...
    type_entreprise NVARCHAR(50) NULL, -- Micro, PME, ETI, GE

    -- Metadata
    date_creation DATETIME2 DEFAULT GETDATE(),
    date_modification DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT UK_dim_activite UNIQUE (naf_section_code, forme_juridique_code)
);
GO

CREATE INDEX IX_dim_activite_naf ON dwh.dim_activite(naf_section_code);
CREATE INDEX IX_dim_activite_secteur ON dwh.dim_activite(secteur_activite);
GO

-- ============================================================
-- DIMENSION INDICATEUR (pour FILOSOFI et autres mesures)
-- ============================================================
IF OBJECT_ID('dwh.dim_indicateur', 'U') IS NOT NULL DROP TABLE dwh.dim_indicateur;
GO

CREATE TABLE dwh.dim_indicateur (
    indicateur_id INT IDENTITY(1,1) PRIMARY KEY,

    indicateur_code NVARCHAR(50) NOT NULL,
    indicateur_libelle NVARCHAR(255) NULL,
    indicateur_description NVARCHAR(MAX) NULL,

    unite_mesure NVARCHAR(50) NULL, -- EUR, %, ratio, nombre
    domaine NVARCHAR(50) NULL, -- REVENUS, PAUVRETE, LOGEMENT, EMPLOI
    source_donnees NVARCHAR(100) NULL, -- FILOSOFI, INSEE RP, etc.

    -- Metadata
    date_creation DATETIME2 DEFAULT GETDATE(),
    date_modification DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT UK_dim_indicateur UNIQUE (indicateur_code)
);
GO

CREATE INDEX IX_dim_indicateur_domaine ON dwh.dim_indicateur(domaine);
GO

-- ============================================================
-- DIMENSION TYPE LOGEMENT / MENAGE
-- ============================================================
IF OBJECT_ID('dwh.dim_logement', 'U') IS NOT NULL DROP TABLE dwh.dim_logement;
GO

CREATE TABLE dwh.dim_logement (
    logement_id INT IDENTITY(1,1) PRIMARY KEY,

    -- Type d'occupation
    occupation_code NVARCHAR(20) NULL,
    occupation_libelle NVARCHAR(100) NULL,

    -- Surpeuplement
    surpeuplement_code NVARCHAR(5) NULL,
    surpeuplement_libelle NVARCHAR(50) NULL,

    -- Type de menage
    type_menage_code NVARCHAR(20) NULL,
    type_menage_libelle NVARCHAR(100) NULL,

    -- Composition
    taille_menage_code NVARCHAR(20) NULL,
    taille_menage_libelle NVARCHAR(50) NULL,

    -- Metadata
    date_creation DATETIME2 DEFAULT GETDATE(),
    date_modification DATETIME2 DEFAULT GETDATE()
);
GO

PRINT 'Tables de dimensions creees avec succes';
