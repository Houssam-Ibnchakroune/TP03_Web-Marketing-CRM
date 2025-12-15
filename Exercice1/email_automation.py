"""
Script d'automatisation d'envoi d'emails via l'API Brevo
Exercice 1.1 - API Email Marketing

Ce script :
1. Se connecte à l'API Brevo avec une clé API
2. Lit une liste de nouveaux inscrits depuis inscrits.csv
3. Envoie un email personnalisé à chaque utilisateur
4. Log les réponses API (succès/échecs)
"""

import requests
import csv
import json
import logging
from datetime import datetime
from typing import Dict, List

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_logs.log'),
        logging.StreamHandler()
    ]
)

# Configuration de l'API Brevo
API_KEY = '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'
URL = 'https://api.brevo.com/v3/smtp/email'
SENDER_EMAIL = 'iberria.vivo.tech@gmail.com'
SENDER_NAME = 'Startup Team'


def lire_inscrits(fichier_csv: str) -> List[Dict]:
    """
    Lit le fichier CSV des inscrits et retourne une liste de dictionnaires
    
    Args:
        fichier_csv: Chemin vers le fichier CSV
        
    Returns:
        Liste de dictionnaires contenant les données des inscrits
    """
    inscrits = []
    try:
        with open(fichier_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                inscrits.append(row)
        logging.info(f"{len(inscrits)} inscrits chargés depuis {fichier_csv}")
        return inscrits
    except FileNotFoundError:
        logging.error(f"Fichier {fichier_csv} non trouvé")
        return []
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier: {e}")
        return []


def creer_email_personnalise(prenom: str, date_inscription: str) -> Dict:
    """
    Crée le contenu HTML personnalisé de l'email de bienvenue
    
    Args:
        prenom: Prénom de l'inscrit
        date_inscription: Date d'inscription
        
    Returns:
        Dictionnaire contenant le sujet et le contenu HTML
    """
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f9f9f9;
            }}
            .header {{
                background-color: #4CAF50;
                color: white;
                padding: 20px;
                text-align: center;
                border-radius: 5px 5px 0 0;
            }}
            .content {{
                background-color: white;
                padding: 30px;
                border-radius: 0 0 5px 5px;
            }}
            .button {{
                display: inline-block;
                padding: 12px 24px;
                background-color: #4CAF50;
                color: white;
                text-decoration: none;
                border-radius: 5px;
                margin-top: 20px;
            }}
            .footer {{
                text-align: center;
                padding: 20px;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Bienvenue chez Startup! 🎉</h1>
            </div>
            <div class="content">
                <h2>Bonjour {prenom},</h2>
                <p>Nous sommes ravis de vous accueillir dans notre communauté!</p>
                <p>Votre inscription du {date_inscription} a bien été enregistrée.</p>
                <p>Avec notre plateforme, vous allez pouvoir:</p>
                <ul>
                    <li>✨ Accéder à des contenus exclusifs</li>
                    <li>📊 Suivre vos performances en temps réel</li>
                    <li>🤝 Rejoindre une communauté d'experts</li>
                    <li>🚀 Développer vos compétences</li>
                </ul>
                <p>Pour commencer, cliquez sur le bouton ci-dessous:</p>
                <a href="https://startup.com/commencer" class="button">Commencer maintenant</a>
                <p style="margin-top: 30px;">Si vous avez des questions, n'hésitez pas à nous contacter.</p>
                <p>À très bientôt!</p>
                <p><strong>L'équipe Startup</strong></p>
            </div>
            <div class="footer">
                <p>© 2025 Startup. Tous droits réservés.</p>
                <p>Vous recevez cet email car vous vous êtes inscrit sur notre plateforme.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return {
        'subject': f"Bienvenue {prenom} ! 🎉",
        'htmlContent': html_content
    }


def envoyer_email(email: str, prenom: str, date_inscription: str) -> Dict:
    """
    Envoie un email via l'API Brevo
    
    Args:
        email: Adresse email du destinataire
        prenom: Prénom du destinataire
        date_inscription: Date d'inscription
        
    Returns:
        Dictionnaire contenant le statut de l'envoi et les détails
    """
    # Créer le contenu personnalisé
    email_content = creer_email_personnalise(prenom, date_inscription)
    
    # Construire le payload pour l'API Brevo
    payload = {
        'sender': {
            'email': SENDER_EMAIL,
            'name': SENDER_NAME
        },
        'to': [
            {
                'email': email,
                'name': prenom
            }
        ],
        'subject': email_content['subject'],
        'htmlContent': email_content['htmlContent']
    }
    
    # Headers pour l'authentification
    headers = {
        'api-key': API_KEY,
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }
    
    try:
        # Envoi de la requête POST à l'API Brevo
        response = requests.post(URL, headers=headers, json=payload)
        
        # Traitement de la réponse
        if response.status_code == 201:
            # Succès (201 Created)
            logging.info(f"✅ Email envoyé avec succès à {email} (Code: {response.status_code})")
            return {
                'success': True,
                'email': email,
                'status_code': response.status_code,
                'message_id': response.json().get('messageId', 'N/A'),
                'details': response.json()
            }
        else:
            # Échec
            logging.error(f"❌ Échec d'envoi à {email} (Code: {response.status_code})")
            logging.error(f"Détails: {response.text}")
            return {
                'success': False,
                'email': email,
                'status_code': response.status_code,
                'error': response.text
            }
            
    except requests.exceptions.RequestException as e:
        # Erreur de connexion ou autre exception
        logging.error(f"❌ Erreur de connexion pour {email}: {e}")
        return {
            'success': False,
            'email': email,
            'error': str(e)
        }


def generer_rapport(resultats: List[Dict]) -> None:
    """
    Génère un rapport récapitulatif des envois d'emails
    
    Args:
        resultats: Liste des résultats d'envoi
    """
    succes = sum(1 for r in resultats if r.get('success'))
    echecs = len(resultats) - succes
    
    print("\n" + "="*60)
    print("📊 RAPPORT D'ENVOI D'EMAILS")
    print("="*60)
    print(f"Total d'emails traités : {len(resultats)}")
    print(f"✅ Succès : {succes}")
    print(f"❌ Échecs : {echecs}")
    print(f"Taux de réussite : {(succes/len(resultats)*100):.1f}%")
    print("="*60)
    
    # Sauvegarder le rapport détaillé
    rapport_fichier = f"rapport_email_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(rapport_fichier, 'w', encoding='utf-8') as f:
        json.dump(resultats, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Rapport détaillé sauvegardé dans {rapport_fichier}")


def main():
    """
    Fonction principale du script
    """
    print("🚀 Démarrage du script d'automatisation d'emails")
    print("-" * 60)
    
    # Vérifier la clé API
    if API_KEY == 'votre_clé_api_brevo':
        print("⚠️  ATTENTION: Vous devez configurer votre clé API Brevo!")
        print("   Modifiez la variable API_KEY dans le script.")
        print("   Pour obtenir une clé API:")
        print("   1. Créez un compte sur https://www.brevo.com")
        print("   2. Allez dans Paramètres > Clés API")
        print("   3. Générez une nouvelle clé API")
        logging.warning("Clé API non configurée - mode démonstration")
        print("\n🔄 Le script continuera en mode démonstration...\n")
    
    # Lire la liste des inscrits
    inscrits = lire_inscrits('inscrits.csv')
    
    if not inscrits:
        print("❌ Aucun inscrit trouvé. Vérifiez le fichier inscrits.csv")
        return
    
    # Envoyer les emails
    resultats = []
    print(f"\n📧 Envoi de {len(inscrits)} emails de bienvenue...\n")
    
    for i, inscrit in enumerate(inscrits, 1):
        email = inscrit.get('email', '')
        prenom = inscrit.get('prenom', 'Utilisateur')
        date_inscription = inscrit.get('date_inscription', 'N/A')
        
        print(f"[{i}/{len(inscrits)}] Envoi à {prenom} ({email})...")
        
        resultat = envoyer_email(email, prenom, date_inscription)
        resultats.append(resultat)
        
        # Pause entre les envois pour respecter les rate limits
        # (Important pour éviter de dépasser les quotas API)
        if i < len(inscrits):
            import time
            time.sleep(0.5)  # 500ms de pause
    
    # Générer le rapport final
    generer_rapport(resultats)
    
    print("\n✨ Script terminé!")


if __name__ == "__main__":
    main()
