-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Tables de Faits
-- Projet Data Engineering - Region Hauts-de-France
-- Schema en etoile (Star Schema)
-- ============================================================

-- ============================================================
-- FAIT POPULATION
-- Mesures de population par territoire, age, sexe et PCS
-- ============================================================
IF OBJECT_ID('dwh.fait_population', 'U') IS NOT NULL DROP TABLE dwh.fait_population;
GO

CREATE TABLE dwh.fait_population (
    population_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres (dimensions)
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,
    demo_id INT NOT NULL,

    -- Mesures
    population FLOAT NULL,
    population_hommes FLOAT NULL,
    population_femmes FLOAT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    -- Contraintes
    CONSTRAINT FK_fait_pop_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_pop_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id),
    CONSTRAINT FK_fait_pop_demo FOREIGN KEY (demo_id) REFERENCES dwh.dim_demographie(demo_id)
);
GO

-- Index pour les agregations frequentes
CREATE INDEX IX_fait_pop_temps ON dwh.fait_population(temps_id);
CREATE INDEX IX_fait_pop_geo ON dwh.fait_population(geo_id);
CREATE INDEX IX_fait_pop_demo ON dwh.fait_population(demo_id);
CREATE INDEX IX_fait_pop_composite ON dwh.fait_population(temps_id, geo_id);
GO

-- ============================================================
-- FAIT EVENEMENTS DEMOGRAPHIQUES (Naissances, Deces)
-- ============================================================
IF OBJECT_ID('dwh.fait_evenements_demo', 'U') IS NOT NULL DROP TABLE dwh.fait_evenements_demo;
GO

CREATE TABLE dwh.fait_evenements_demo (
    evenement_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,

    -- Mesures
    naissances INT NULL,
    deces INT NULL,
    solde_naturel AS (naissances - deces) PERSISTED,

    -- Taux calcules (pour 1000 habitants)
    taux_natalite FLOAT NULL,
    taux_mortalite FLOAT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_fait_evt_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_evt_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id)
);
GO

CREATE INDEX IX_fait_evt_temps ON dwh.fait_evenements_demo(temps_id);
CREATE INDEX IX_fait_evt_geo ON dwh.fait_evenements_demo(geo_id);
GO

-- ============================================================
-- FAIT CREATION ENTREPRISES
-- ============================================================
IF OBJECT_ID('dwh.fait_entreprises', 'U') IS NOT NULL DROP TABLE dwh.fait_entreprises;
GO

CREATE TABLE dwh.fait_entreprises (
    entreprise_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,
    activite_id INT NOT NULL,

    -- Mesures - Creations
    nb_creations_entreprises INT NULL,
    nb_creations_etablissements INT NULL,
    nb_creations_micro INT NULL,
    nb_creations_ei INT NULL, -- Entrepreneurs individuels

    -- Mesures par profil createur
    nb_creations_hommes INT NULL,
    nb_creations_femmes INT NULL,
    nb_creations_moins_30ans INT NULL,
    nb_creations_30_39ans INT NULL,
    nb_creations_40_49ans INT NULL,
    nb_creations_50_59ans INT NULL,
    nb_creations_60ans_plus INT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_fait_ent_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_ent_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id),
    CONSTRAINT FK_fait_ent_activite FOREIGN KEY (activite_id) REFERENCES dwh.dim_activite(activite_id)
);
GO

CREATE INDEX IX_fait_ent_temps ON dwh.fait_entreprises(temps_id);
CREATE INDEX IX_fait_ent_geo ON dwh.fait_entreprises(geo_id);
CREATE INDEX IX_fait_ent_activite ON dwh.fait_entreprises(activite_id);
CREATE INDEX IX_fait_ent_composite ON dwh.fait_entreprises(temps_id, geo_id, activite_id);
GO

-- ============================================================
-- FAIT EMPLOI ET CHOMAGE
-- ============================================================
IF OBJECT_ID('dwh.fait_emploi', 'U') IS NOT NULL DROP TABLE dwh.fait_emploi;
GO

CREATE TABLE dwh.fait_emploi (
    emploi_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,
    demo_id INT NOT NULL,

    -- Mesures - Population active
    population_active FLOAT NULL,
    population_en_emploi FLOAT NULL,
    population_chomeurs FLOAT NULL,

    -- Taux calcules
    taux_activite FLOAT NULL,
    taux_emploi FLOAT NULL,
    taux_chomage FLOAT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_fait_emploi_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_emploi_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id),
    CONSTRAINT FK_fait_emploi_demo FOREIGN KEY (demo_id) REFERENCES dwh.dim_demographie(demo_id)
);
GO

CREATE INDEX IX_fait_emploi_temps ON dwh.fait_emploi(temps_id);
CREATE INDEX IX_fait_emploi_geo ON dwh.fait_emploi(geo_id);
CREATE INDEX IX_fait_emploi_demo ON dwh.fait_emploi(demo_id);
GO

-- ============================================================
-- FAIT REVENUS (FILOSOFI)
-- ============================================================
IF OBJECT_ID('dwh.fait_revenus', 'U') IS NOT NULL DROP TABLE dwh.fait_revenus;
GO

CREATE TABLE dwh.fait_revenus (
    revenu_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,
    demo_id INT NULL, -- Optionnel si par tranche d'age

    -- Mesures - Revenus
    revenu_median FLOAT NULL, -- MED_SL
    revenu_d1 FLOAT NULL, -- Premier decile
    revenu_d9 FLOAT NULL, -- Neuvieme decile
    rapport_interdecile FLOAT NULL, -- D9/D1

    -- Mesures - Pauvrete
    taux_pauvrete FLOAT NULL, -- PR_MD60
    nb_menages INT NULL,
    nb_personnes INT NULL,

    -- Structure des revenus (%)
    part_revenus_activite FLOAT NULL,
    part_pensions_retraites FLOAT NULL,
    part_prestations_sociales FLOAT NULL,
    part_revenus_patrimoine FLOAT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_fait_rev_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_rev_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id),
    CONSTRAINT FK_fait_rev_demo FOREIGN KEY (demo_id) REFERENCES dwh.dim_demographie(demo_id)
);
GO

CREATE INDEX IX_fait_rev_temps ON dwh.fait_revenus(temps_id);
CREATE INDEX IX_fait_rev_geo ON dwh.fait_revenus(geo_id);
GO

-- ============================================================
-- FAIT LOGEMENT
-- ============================================================
IF OBJECT_ID('dwh.fait_logement', 'U') IS NOT NULL DROP TABLE dwh.fait_logement;
GO

CREATE TABLE dwh.fait_logement (
    logement_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,
    logement_dim_id INT NULL,

    -- Mesures - Stock logements
    nb_residences_principales FLOAT NULL,
    nb_logements_surpeuples FLOAT NULL,
    nb_logements_normaux FLOAT NULL,

    -- Taux calcules
    taux_surpeuplement FLOAT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_fait_log_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_log_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id),
    CONSTRAINT FK_fait_log_dim FOREIGN KEY (logement_dim_id) REFERENCES dwh.dim_logement(logement_id)
);
GO

CREATE INDEX IX_fait_log_temps ON dwh.fait_logement(temps_id);
CREATE INDEX IX_fait_log_geo ON dwh.fait_logement(geo_id);
GO

-- ============================================================
-- FAIT MENAGES
-- ============================================================
IF OBJECT_ID('dwh.fait_menages', 'U') IS NOT NULL DROP TABLE dwh.fait_menages;
GO

CREATE TABLE dwh.fait_menages (
    menage_id BIGINT IDENTITY(1,1) PRIMARY KEY,

    -- Cles etrangeres
    temps_id INT NOT NULL,
    geo_id INT NOT NULL,
    demo_id INT NULL,
    logement_dim_id INT NULL,

    -- Mesures
    nb_menages FLOAT NULL,
    nb_personnes FLOAT NULL,
    taille_moyenne_menage FLOAT NULL,

    -- Metadata
    source_fichier NVARCHAR(255) NULL,
    date_chargement DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_fait_men_temps FOREIGN KEY (temps_id) REFERENCES dwh.dim_temps(temps_id),
    CONSTRAINT FK_fait_men_geo FOREIGN KEY (geo_id) REFERENCES dwh.dim_geographie(geo_id),
    CONSTRAINT FK_fait_men_demo FOREIGN KEY (demo_id) REFERENCES dwh.dim_demographie(demo_id),
    CONSTRAINT FK_fait_men_log FOREIGN KEY (logement_dim_id) REFERENCES dwh.dim_logement(logement_id)
);
GO

CREATE INDEX IX_fait_men_temps ON dwh.fait_menages(temps_id);
CREATE INDEX IX_fait_men_geo ON dwh.fait_menages(geo_id);
GO

PRINT 'Tables de faits creees avec succes';
