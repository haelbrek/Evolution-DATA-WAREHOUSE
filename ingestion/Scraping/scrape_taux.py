# -*- coding: utf-8 -*-
"""Scraper pour le barometre des taux Meilleurtaux."""

import argparse
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import os
import smtplib
from email.message import EmailMessage

import pandas as pd
import requests

BASE_URL = "https://www.meilleurtaux.com/ajax_requete/ajax_barometre.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Referer": "https://www.meilleurtaux.com/credit-immobilier/barometre-des-taux.html",
    "X-Requested-With": "XMLHttpRequest",
}

DURATIONS = [7, 10, 15, 20, 25]

REGIONS = {
    "0": "National",
    "2": "Region Nord",
    "3": "Region Ouest",
    "4": "Region Sud Ouest",
    "5": "Region Sud Est",
    "6": "Region Rhone Alpes",
    "7": "Region Est",
    "8": "PARIS IDF",
}

DEFAULT_OUTPUT_PATH = Path("uploads") / "landing" / "excel" / "taux_meilleurtaux.xlsx"

def fetch_region_data(region_code: str) -> Dict[str, str]:
    response = requests.get(BASE_URL, params={"z": region_code}, headers=HEADERS, timeout=10)
    response.raise_for_status()
    payload = response.json()
    results = payload.get("res")
    if not results:
        raise ValueError(f"Aucune donnee retournee pour la region {region_code!r}.")
    return results[0]

def extract_rates(raw_entry: Dict[str, str], duration: int) -> Dict[str, float]:
    suffix = f"{duration}f"
    excellent = raw_entry.get(f"e{suffix}")
    tres_bon = raw_entry.get(f"b{suffix}")
    bon = raw_entry.get(f"m{suffix}")
    if excellent is None or tres_bon is None or bon is None:
        raise KeyError(f"Taux manquants pour la duree {duration} ans.")
    def to_float(value: str) -> float:
        value = value.replace(",", ".")
        return round(float(value), 2)
    return {
        "Taux excellent": to_float(excellent),
        "Tres bon taux": to_float(tres_bon),
        "Bon taux": to_float(bon),
    }

def build_dataset() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for region_code, region_name in REGIONS.items():
        raw_entry = fetch_region_data(region_code)
        update_date_raw = raw_entry.get("date")
        if update_date_raw:
            try:
                update_date = datetime.strptime(update_date_raw, "%Y-%m-%d").strftime("%d/%m/%Y")
            except ValueError:
                update_date = update_date_raw
        else:
            update_date = None
        for duration in DURATIONS:
            rates = extract_rates(raw_entry, duration)
            rows.append(
                {
                    "Region": region_name,
                    "Duree (ans)": duration,
                    "Date de mise a jour": update_date,
                    **rates,
                }
            )
    return rows

def resolve_smtp_password(args: argparse.Namespace) -> str | None:
    password = None
    if args.smtp_password_env:
        password = os.getenv(args.smtp_password_env)
        if password is None:
            raise ValueError(f"Variable d'environnement {args.smtp_password_env!r} introuvable pour le mot de passe SMTP.")
    elif args.smtp_password:
        password = args.smtp_password
    return password

def send_email_with_attachment(output_path: Path, args: argparse.Namespace) -> None:
    password = resolve_smtp_password(args)
    if password is None:
        raise ValueError("Aucun mot de passe SMTP disponible. Utilisez --smtp-password ou --smtp-password-env.")
    message = EmailMessage()
    message["Subject"] = args.email_subject or "Barometre des taux Meilleurtaux"
    message["From"] = args.smtp_user
    message["To"] = args.email_recipient
    body = args.email_body or "Veuillez trouver ci-joint le fichier des taux Meilleurtaux."
    message.set_content(body)
    data = output_path.read_bytes()
    message.add_attachment(
        data,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=output_path.name,
    )
    timeout = args.smtp_timeout
    if args.smtp_use_ssl:
        with smtplib.SMTP_SSL(args.smtp_server, args.smtp_port, timeout=timeout) as server:
            server.login(args.smtp_user, password)
            server.send_message(message)
    else:
        with smtplib.SMTP(args.smtp_server, args.smtp_port, timeout=timeout) as server:
            server.ehlo()
            server.starttls()
            server.login(args.smtp_user, password)
            server.send_message(message)

def save_to_excel(rows: List[Dict[str, object]], output_path: Path) -> None:
    df = pd.DataFrame(rows)
    df.sort_values(["Region", "Duree (ans)"], inplace=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape les taux immobiliers Meilleurtaux.")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Chemin du fichier Excel de sortie (defaut: uploads/landing/excel/taux_meilleurtaux.xlsx).",
    )
    parser.add_argument("--email-recipient", default=None, help="Adresse e-mail destinataire.")
    parser.add_argument("--email-subject", default=None, help="Sujet personnalise du message.")
    parser.add_argument("--email-body", default=None, help="Corps personnalise du message.")
    parser.add_argument("--smtp-server", default=None, help="Hote SMTP (ex: smtp.gmail.com).")
    parser.add_argument("--smtp-port", default=587, type=int, help="Port SMTP (defaut: 587).")
    parser.add_argument("--smtp-user", default=None, help="Identifiant SMTP / adresse d'expedition.")
    parser.add_argument("--smtp-password-env", default=None, help="Nom de la variable d'environnement contenant le mot de passe SMTP.")
    parser.add_argument("--smtp-password", default=None, help="Mot de passe SMTP (deconseille, privilegier la variable d'environnement).")
    parser.add_argument("--smtp-use-ssl", action="store_true", help="Utiliser SMTP SSL direct au lieu de STARTTLS.")
    parser.add_argument("--smtp-timeout", default=30, type=int, help="Delai d'attente (s) pour les operations SMTP.")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    rows = build_dataset()
    save_to_excel(rows, args.output)
    print(f"Fichier enregistre : {args.output.resolve()}")
    should_email = all([
        args.email_recipient,
        args.smtp_server,
        args.smtp_user,
    ])
    if should_email:
        try:
            send_email_with_attachment(args.output, args)
            print(f"E-mail envoye a {args.email_recipient}.")
        except Exception as exc:
            print(f"Echec de l'envoi de l'e-mail : {exc}")
    elif args.email_recipient:
        print("Configuration SMTP incomplete : l'e-mail n'a pas ete envoye.")
if __name__ == "__main__":
    main()
