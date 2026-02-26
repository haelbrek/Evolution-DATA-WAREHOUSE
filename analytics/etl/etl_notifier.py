#!/usr/bin/env python3
"""
E6 - Notifications email du pipeline ETL
Projet Data Engineering - Region Hauts-de-France

Configuration via variables d'environnement :
    ETL_SMTP_HOST      : Serveur SMTP        (defaut: smtp.gmail.com)
    ETL_SMTP_PORT      : Port SMTP           (defaut: 587)
    ETL_SMTP_USER      : Email expediteur    (ex: moncompte@gmail.com)
    ETL_SMTP_PASSWORD  : Mot de passe SMTP   (App Password Gmail recommande)
    ETL_NOTIFY_EMAIL   : Email destinataire  (ex: hamza@example.com)

Usage Gmail :
    1. Activer la validation en 2 etapes sur votre compte Google
    2. Generer un "App Password" dans Securite > Mots de passe des applications
    3. Utiliser ce mot de passe dans ETL_SMTP_PASSWORD (pas votre mot de passe Google)
"""

import os
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

logger = logging.getLogger('etl_notifier')


def _load_tfvars() -> dict:
    """Lit terraform.tfvars et retourne les variables sous forme de dict."""
    tfvars_path = Path(__file__).parents[2] / 'Terraform' / 'terraform.tfvars'
    values = {}
    try:
        with open(tfvars_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, val = line.partition('=')
                values[key.strip()] = val.strip().strip('"')
    except FileNotFoundError:
        logger.warning(f"terraform.tfvars non trouve : {tfvars_path}")
    return values


def get_smtp_config() -> dict:
    """
    Charge la configuration SMTP depuis terraform.tfvars.
    Les variables d'environnement ont priorite si definies.
    """
    tf = _load_tfvars()
    return {
        'host':     os.getenv('ETL_SMTP_HOST',     tf.get('etl_smtp_host',     'smtp.gmail.com')),
        'port':     int(os.getenv('ETL_SMTP_PORT', tf.get('etl_smtp_port',     '587'))),
        'user':     os.getenv('ETL_SMTP_USER',     tf.get('etl_smtp_user',     '')),
        'password': os.getenv('ETL_SMTP_PASSWORD', tf.get('etl_smtp_password', '')),
        'to':       os.getenv('ETL_NOTIFY_EMAIL',  tf.get('etl_notify_email',  '')),
    }


def _send_email(subject: str, body_html: str, smtp_config: dict) -> bool:
    """Envoie un email via SMTP TLS. Retourne True si succes."""
    if not smtp_config.get('user') or not smtp_config.get('to'):
        logger.warning(
            "Notification email ignoree : ETL_SMTP_USER ou ETL_NOTIFY_EMAIL non configure"
        )
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From']    = smtp_config['user']
        msg['To']      = smtp_config['to']
        msg.attach(MIMEText(body_html, 'html', 'utf-8'))

        with smtplib.SMTP(smtp_config['host'], smtp_config['port']) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_config['user'], smtp_config['password'])
            server.sendmail(smtp_config['user'], smtp_config['to'], msg.as_string())

        logger.info(f"Email envoye a {smtp_config['to']} : {subject}")
        return True

    except Exception as e:
        logger.error(f"Echec envoi email : {e}")
        return False


def _statut_icone(statut: str) -> str:
    return {'OK': '✅', 'SKIP': '⏭️', 'IGNORE': '⏭️', 'ERREUR': '❌'}.get(statut, '❓')


def _statut_couleur(statut: str) -> str:
    return {
        'OK':     '#d4edda',
        'SKIP':   '#fff3cd',
        'IGNORE': '#e2e3e5',
        'ERREUR': '#f8d7da',
    }.get(statut, '#ffffff')


def _build_detail_rows(rapport_tables: dict) -> str:
    """Construit les lignes HTML du tableau de detail par table."""
    rows = ''
    for table, info in rapport_tables.items():
        statut  = info.get('statut', 'INCONNU')
        icone   = _statut_icone(statut)
        couleur = _statut_couleur(statut)
        nb      = info.get('nb_lignes', '-')
        heure   = info.get('heure', '-')
        duree   = f"{info.get('duree_sec', 0):.1f}s" if info.get('duree_sec') else '-'
        erreur  = info.get('erreur', '')
        detail  = (
            f"<span style='color:#721c24;font-family:monospace;font-size:11px'>"
            f"{str(erreur)[:150]}</span>"
            if erreur else ''
        )
        rows += f"""
        <tr style="background-color:{couleur}">
            <td style="padding:6px 10px">{table}</td>
            <td style="padding:6px 10px;text-align:center">{icone} {statut}</td>
            <td style="padding:6px 10px;text-align:center">{nb}</td>
            <td style="padding:6px 10px;text-align:center">{heure}</td>
            <td style="padding:6px 10px;text-align:center">{duree}</td>
            <td style="padding:6px 10px">{detail}</td>
        </tr>"""
    return rows


def _build_etape_rows(rapport_etapes: dict) -> str:
    """Construit les lignes HTML du tableau des etapes principales."""
    rows = ''
    for etape, info in rapport_etapes.items():
        if etape == 'details':
            continue
        statut = info.get('statut', '?')
        icone  = _statut_icone(statut)
        duree  = f"{info.get('duree_sec', 0):.1f}s" if info.get('duree_sec') else '-'
        nb     = info.get('nb_lignes', '-')
        rows += f"""
        <tr style="background-color:{_statut_couleur(statut)}">
            <td style="padding:6px 10px"><b>{etape.capitalize()}</b></td>
            <td style="padding:6px 10px;text-align:center">{icone} {statut}</td>
            <td style="padding:6px 10px;text-align:center">{nb}</td>
            <td style="padding:6px 10px;text-align:center">{duree}</td>
        </tr>"""
    return rows


_HEADER_STYLE = "background-color:#343a40;color:white;padding:8px"
_TABLE_STYLE  = "border='1' cellspacing='0' style='border-collapse:collapse;width:100%'"


def send_success_email(rapport_etapes: dict, smtp_config: dict = None) -> bool:
    """
    Envoie un email de succes avec le recapitulatif complet du pipeline.

    rapport_etapes = {
        'staging':    {'statut': 'OK', 'nb_lignes': 0, 'duree_sec': 12.3},
        'dimensions': {'statut': 'OK', 'nb_lignes': 85, 'duree_sec': 4.1},
        'faits':      {'statut': 'OK', 'nb_lignes': 4200, 'duree_sec': 8.7},
        'backup':     {'statut': 'OK', 'nb_lignes': 0, 'duree_sec': 45.0},
        'details': {
            'dwh.dim_geographie':  {'statut': 'OK',   'nb_lignes': 250, 'heure': '14:32:05', ...},
            'dwh.fait_emploi':     {'statut': 'SKIP',  'nb_lignes': 0,   'heure': '14:32:18', ...},
            'dwh.fait_menages':    {'statut': 'IGNORE','nb_lignes': 0,   'heure': '14:32:20',
                                    'erreur': 'Tables staging manquantes: stg_menages'},
        }
    }
    """
    if smtp_config is None:
        smtp_config = get_smtp_config()

    now         = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    etape_rows  = _build_etape_rows(rapport_etapes)
    details     = rapport_etapes.get('details', {})
    detail_rows = _build_detail_rows(details) if details else (
        '<tr><td colspan="6" style="padding:8px;text-align:center">Aucun detail</td></tr>'
    )

    body = f"""
    <html><body style="font-family:Arial,sans-serif;margin:20px;color:#212529">

    <h2 style="color:#28a745">&#x2705; ETL Data Warehouse &mdash; Chargement reussi</h2>
    <p>
        <b>Date :</b> {now}<br>
        <b>Projet :</b> Data Engineering &mdash; Region Hauts-de-France
    </p>

    <h3>Recapitulatif des etapes</h3>
    <table {_TABLE_STYLE}>
        <tr>
            <th style="{_HEADER_STYLE}">Etape</th>
            <th style="{_HEADER_STYLE}">Statut</th>
            <th style="{_HEADER_STYLE}">Lignes</th>
            <th style="{_HEADER_STYLE}">Duree</th>
        </tr>
        {etape_rows}
    </table>

    <h3 style="margin-top:24px">Detail par table</h3>
    <table {_TABLE_STYLE}>
        <tr>
            <th style="{_HEADER_STYLE}">Table</th>
            <th style="{_HEADER_STYLE}">Statut</th>
            <th style="{_HEADER_STYLE}">Lignes</th>
            <th style="{_HEADER_STYLE}">Heure</th>
            <th style="{_HEADER_STYLE}">Duree</th>
            <th style="{_HEADER_STYLE}">Detail</th>
        </tr>
        {detail_rows}
    </table>

    <hr style="margin-top:24px">
    <small style="color:#6c757d">ETL Pipeline &mdash; Data Warehouse Hauts-de-France</small>
    </body></html>
    """

    subject = f"\u2705 ETL DWH \u2014 Succes \u2014 {now}"
    return _send_email(subject, body, smtp_config)


def send_error_email(
    etape: str,
    table: str,
    erreur: str,
    heure: str,
    rapport_partiel: dict = None,
    smtp_config: dict = None
) -> bool:
    """
    Envoie un email d'alerte en cas d'erreur ETL.

    etape          : nom de l'etape  (ex: 'Faits', 'Staging', 'Dimensions')
    table          : table concernee (ex: 'dwh.fait_revenus', 'stg_emploi_chomage')
    erreur         : message d'erreur complet
    heure          : heure de l'erreur au format HH:MM:SS
    rapport_partiel: dict des tables deja traitees avant l'erreur (meme format que details)
    """
    if smtp_config is None:
        smtp_config = get_smtp_config()

    now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    detail_section = ''
    if rapport_partiel:
        rows = _build_detail_rows(rapport_partiel)
        detail_section = f"""
        <h3 style="margin-top:24px">Tables traitees avant l'erreur</h3>
        <table {_TABLE_STYLE}>
            <tr>
                <th style="{_HEADER_STYLE}">Table</th>
                <th style="{_HEADER_STYLE}">Statut</th>
                <th style="{_HEADER_STYLE}">Lignes</th>
                <th style="{_HEADER_STYLE}">Heure</th>
                <th style="{_HEADER_STYLE}">Duree</th>
                <th style="{_HEADER_STYLE}">Detail</th>
            </tr>
            {rows}
        </table>"""

    body = f"""
    <html><body style="font-family:Arial,sans-serif;margin:20px;color:#212529">

    <h2 style="color:#dc3545">&#x274C; ETL Data Warehouse &mdash; Erreur de chargement</h2>
    <p>
        <b>Date :</b> {now}<br>
        <b>Projet :</b> Data Engineering &mdash; Region Hauts-de-France
    </p>

    <table style="background-color:#f8d7da;border:1px solid #f5c6cb;
                  border-radius:4px;width:100%;border-collapse:collapse">
        <tr>
            <td style="padding:8px 12px;width:120px"><b>Etape</b></td>
            <td style="padding:8px 12px">{etape}</td>
        </tr>
        <tr>
            <td style="padding:8px 12px"><b>Table</b></td>
            <td style="padding:8px 12px">{table}</td>
        </tr>
        <tr>
            <td style="padding:8px 12px"><b>Heure</b></td>
            <td style="padding:8px 12px">{heure}</td>
        </tr>
        <tr>
            <td style="padding:8px 12px;vertical-align:top"><b>Erreur</b></td>
            <td style="padding:8px 12px;color:#721c24;font-family:monospace;font-size:12px">
                {str(erreur)[:800]}
            </td>
        </tr>
    </table>

    {detail_section}

    <hr style="margin-top:24px">
    <small style="color:#6c757d">ETL Pipeline &mdash; Data Warehouse Hauts-de-France</small>
    </body></html>
    """

    subject = f"\u274C ETL DWH \u2014 ERREUR {etape} ({table}) \u2014 {heure}"
    return _send_email(subject, body, smtp_config)
