

import psycopg2
import requests
from datetime import datetime, timedelta
import logging
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MatomoETL:
    
    def __init__(self):
        """
        Initialiser l'ETL
        """
        
        # Configuration PostgreSQL
        self.db_config = {
            'host': 'localhost',
            'database': 'TP3_DB',
            'user': 'postgres',
            'password': '0000', 
            'port': '5432'
        }
        
        # Configuration Matomo
        #  VOTRE MATOMO (modifiez ces valeurs)
        self.matomo_url = 'https://startupteamcom.matomo.cloud/index.php'
        self.matomo_token = '9536e21d3aed48f7961b4b66027a939f'
        self.matomo_site_id = '1'
        logger.info(" Mode LOCAL activé")
        
        self.conn = None
        self.simulation_mode = True
        logger.info(" MODE SIMULATION activé (pas de trafic réel Matomo)")

    def connect_db(self):
        """Connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(" Connecté à PostgreSQL")
        except Exception as e:
            logger.error(f" Erreur de connexion DB: {e}")
            raise
    
    def close_db(self):
        """Fermer la connexion"""
        if self.conn:
            self.conn.close()
            logger.info(" Connexion fermée")
    
    # ============================================
    # EXTRACT - Appeler l'API Matomo RÉELLE
    # ============================================
    
    
    
    def call_matomo_api(self, method: str, extra_params: dict = None) -> dict:
        """
        Appeler l'API Matomo
        
        Args:
            method: Méthode API (ex: 'VisitsSummary.get')
            extra_params: Paramètres supplémentaires
        """
        params = {
            'module': 'API',
            'method': method,
            'idSite': self.matomo_site_id,
            'period': 'day',
            'date': 'today',
            'format': 'JSON',
            'token_auth': self.matomo_token
        }
        
        if extra_params:
            params.update(extra_params)
        
        try:
            logger.info(f" Appel API: {method}")
            response = requests.get(self.matomo_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Vérifier si erreur
            if isinstance(data, dict) and 'result' in data and data['result'] == 'error':
                logger.error(f" Erreur API Matomo: {data.get('message', 'Unknown')}")
                return None
            
            logger.info(f"   ✓ Données récupérées")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f" Erreur requête: {e}")
            return None
        

    
    
    def simulate_visits_summary(self, date: str) -> dict:
        """Simuler les métriques globales"""
        
        visits = random.randint(80, 400)
        conversions = random.randint(5, int(visits * 0.25))
        
        data = {
            'nb_visits': visits,
            'nb_uniq_visitors': int(visits * random.uniform(0.6, 0.9)),
            'nb_actions': visits * random.randint(2, 6),
            'nb_conversions': conversions,
            'conversion_rate': f"{round((conversions / visits) * 100, 2)}%",
            'bounce_rate': f"{round(random.uniform(20, 55), 2)}%",
            'avg_time_on_site': random.randint(60, 600),
            'nb_actions_per_visit': round(random.uniform(2.0, 5.5), 2)
        }
        
        logger.info(f"   → {visits} visites simulées, {conversions} conversions")
        return data

    
    def simulate_channels(self, date: str) -> list:
        """Simuler les canaux marketing"""
        
        channels = ['organic', 'paid', 'email', 'social', 'direct']
        total_visits = random.randint(80, 400)
        
        simulated = []
        remaining_visits = total_visits
        
        for channel in channels:
            visits = random.randint(10, max(10, remaining_visits // 2))
            conversions = random.randint(0, int(visits * 0.3))
            revenue = round(conversions * random.uniform(20, 120), 2)
            
            simulated.append({
                'date': datetime.strptime(date, '%Y-%m-%d').date(),
                'channel': channel,
                'visits': visits,
                'conversions': conversions,
                'revenue': revenue
            })
            
            remaining_visits -= visits
            if remaining_visits <= 0:
                break
        
        logger.info(f"   → {len(simulated)} canaux simulés")
        return simulated

    
    def extract_devices(self, date: str) -> list:
        """
        Extraire les types d'appareils (bonus)
        
        API: DevicesDetection.getType
        """
        logger.info(f"📥 EXTRACT - Appareils pour {date}")
        
        data = self.call_matomo_api(
            'DevicesDetection.getType',
            {'date': date}
        )
        
        if not data or not isinstance(data, list):
            return []
        
        logger.info(f"   → {len(data)} types d'appareils")
        return data
    
    # ============================================
    # TRANSFORM - Nettoyer les données Matomo
    # ============================================
    
    def transform_summary(self, data: dict, date: str) -> dict:
        """Transformer le résumé des visites"""
        logger.info(f" TRANSFORM - Nettoyage du résumé")
        
        if not data:
            return None
        
        transformed = {
            'date': datetime.strptime(date, '%Y-%m-%d').date(),
            'visits': int(data.get('nb_visits', 0)),
            'unique_visitors': int(data.get('nb_uniq_visitors', 0)),
            'actions': int(data.get('nb_actions', 0)),
            'conversions': int(data.get('nb_conversions', 0)),
            'conversion_rate': float(str(data.get('conversion_rate', 0)).replace('\xa0', '').replace('%', '') or 0),
            'bounce_rate': float(str(data.get('bounce_rate', 0)).replace('\xa0', '').replace('%', '') or 0),
            'avg_time_on_site': int(data.get('avg_time_on_site', 0)),
            'pages_per_visit': float(data.get('nb_actions_per_visit', 0))
        }
        
        logger.info(f"   ✓ Résumé transformé")
        return transformed
    
    def transform_channels(self, channels: list, date: str) -> list:
        """
        Transformer les canaux Matomo vers notre format
        
        Matomo utilise:
        - 'search' → 'organic'
        - 'direct' → 'direct'
        - 'website' → 'referral'
        - 'campaign' → 'paid'
        - 'social' → 'social'
        """
        logger.info(f" TRANSFORM - Nettoyage des canaux")
        
        if not channels:
            return []
        
        # Mapping des canaux Matomo vers nos canaux
        channel_mapping = {
            'search': 'organic',
            'direct': 'direct',
            'website': 'referral',
            'campaign': 'paid',
            'social': 'social'
        }
        
        transformed = []
        for channel in channels:
            label = channel.get('label', 'unknown').lower()
            mapped_channel = channel_mapping.get(label, 'other')
            
            transformed.append({
                'date': datetime.strptime(date, '%Y-%m-%d').date(),
                'channel': mapped_channel,
                'visits': int(channel.get('nb_visits', 0)),
                'conversions': int(channel.get('nb_conversions', 0)),
                'revenue': float(channel.get('revenue', 0))
            })
        
        logger.info(f"   ✓ {len(transformed)} canaux transformés")
        return transformed
    
    # ============================================
    # LOAD - Charger dans PostgreSQL
    # ============================================
    
    def load_summary(self, summary: dict):
        """Charger le résumé dans daily_metrics"""
        if not summary:
            logger.warning("  Pas de résumé à charger")
            return
        
        logger.info(f" LOAD - Insertion dans daily_metrics")
        
        query = """
        INSERT INTO daily_metrics (
            date, visits, unique_visitors, actions, conversions,
            conversion_rate, bounce_rate, avg_time_on_site, pages_per_visit
        )
        VALUES (
            %(date)s, %(visits)s, %(unique_visitors)s, %(actions)s, %(conversions)s,
            %(conversion_rate)s, %(bounce_rate)s, %(avg_time_on_site)s, %(pages_per_visit)s
        )
        ON CONFLICT (date) DO UPDATE SET
            visits = EXCLUDED.visits,
            unique_visitors = EXCLUDED.unique_visitors,
            actions = EXCLUDED.actions,
            conversions = EXCLUDED.conversions,
            conversion_rate = EXCLUDED.conversion_rate,
            bounce_rate = EXCLUDED.bounce_rate,
            avg_time_on_site = EXCLUDED.avg_time_on_site,
            pages_per_visit = EXCLUDED.pages_per_visit
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, summary)
            self.conn.commit()
            logger.info(f"   ✓ Résumé chargé")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"   ✗ Erreur: {e}")
            raise
    
    def load_channels(self, channels: list):
        """Charger les canaux dans channel_metrics"""
        if not channels:
            logger.warning("  Pas de canaux à charger")
            return
        
        logger.info(f" LOAD - Insertion dans channel_metrics")
        
        query = """
        INSERT INTO channel_metrics (date, channel, visits, conversions, revenue)
        VALUES (%(date)s, %(channel)s, %(visits)s, %(conversions)s, %(revenue)s)
        ON CONFLICT (date, channel) DO UPDATE SET
            visits = EXCLUDED.visits,
            conversions = EXCLUDED.conversions,
            revenue = EXCLUDED.revenue
        """
        
        try:
            with self.conn.cursor() as cur:
                for channel in channels:
                    cur.execute(query, channel)
            self.conn.commit()
            logger.info(f"   ✓ {len(channels)} canaux chargés")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"   ✗ Erreur: {e}")
            raise
    
    # ============================================
    # PIPELINE COMPLET
    # ============================================
    
    def run_pipeline(self, date: str = None):
        """Exécuter le pipeline ETL complet"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        print("\n" + "="*60)
        print(f" PIPELINE ETL MATOMO - {date}")
        print("="*60 + "\n")

        try:
            # Connexion DB
            self.connect_db()

            # =====================================
            # 1. EXTRACT
            # =====================================
            print(" Phase 1 : EXTRACTION")
            print("-" * 60)

            if self.simulation_mode:
                summary_data = self.simulate_visits_summary(date)
                channels = self.simulate_channels(date)
                channels_data = None
            else:
                summary_data = self.extract_visits_summary(date)
                channels_data = self.extract_channels(date)
                channels = self.transform_channels(channels_data, date)

            if not summary_data:
                print("  Aucune donnée disponible pour cette date")
                return

            # =====================================
            # 2. TRANSFORM
            # =====================================
            print("\n Phase 2 : TRANSFORMATION")
            print("-" * 60)

            summary = self.transform_summary(summary_data, date)

            # Les canaux sont déjà transformés en mode simulation
            if not self.simulation_mode:
                channels = self.transform_channels(channels_data, date)

            # =====================================
            # 3. LOAD
            # =====================================
            print("\n Phase 3 : CHARGEMENT dans PostgreSQL")
            print("-" * 60)

            self.load_summary(summary)
            self.load_channels(channels)

            # =====================================
            # RÉSUMÉ
            # =====================================
            print("\n" + "="*60)
            print(" PIPELINE TERMINÉ AVEC SUCCÈS!")
            print("="*60)

            print(f"\n Résumé:")
            print(f"   • Date           : {date}")
            print(f"   • Visites        : {summary['visits']}")
            print(f"   • Conversions    : {summary['conversions']}")
            print(f"   • Taux conversion: {summary['conversion_rate']}%")
            print(f"   • Canaux         : {len(channels)}")

        except Exception as e:
            print("\n ERREUR DANS LE PIPELINE!")
            print(f"   {e}")
            raise

        finally:
            self.close_db()

    def run_historical(self, days: int = 7):
        """Exécuter pour les N derniers jours"""
        print("\n" + "="*60)
        print(f" IMPORT HISTORIQUE - {days} jours")
        print("="*60 + "\n")
        
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            try:
                self.run_pipeline(date)
            except Exception as e:
                logger.error(f"Erreur pour {date}: {e}")
                continue
    
    def test_connection(self):
        """Tester la connexion à Matomo"""
        print("\n" + "="*60)
        print(" TEST DE CONNEXION MATOMO")
        print("="*60 + "\n")
        
        try:
            # Tester avec API.getMatomoVersion
            version_data = self.call_matomo_api('API.getMatomoVersion')
            
            if version_data:
                version = version_data.get('value', 'Inconnue')
                print(f" Connexion réussie!")
                print(f"   Version Matomo: {version}")
                print(f"   URL: {self.matomo_url}")
                print(f"   Site ID: {self.matomo_site_id}")
                return True
            else:
                print(" Connexion échouée")
                print(" Vérifiez:")
                print("   - L'URL Matomo est correcte")
                print("   - Le token est valide")
                print("   - Le Site ID existe")
                return False
                
        except Exception as e:
            print(f" Erreur: {e}")
            return False


def main():
    """Point d'entrée principal"""
    
    print("\n" + "="*60)
    print(" PIPELINE ETL MATOMO")
    print("="*60)
    print()
    
    # Créer l'instance ETL
   
    etl = MatomoETL()
    
    # Tester la connexion
    if not etl.test_connection():
        print("\n Impossible de continuer sans connexion Matomo valide")
        return
    
    # Menu d'actions
    print("\n" + "-"*60)
    print("Que voulez-vous faire ?")
    print("  1. Exécuter pour aujourd'hui")
    print("  2. Importer les 7 derniers jours")
    print("  3. Importer les 30 derniers jours")
    print()
    
    action = input("Votre choix (1-3): ").strip()
    
    if action == '1':
        etl.run_pipeline()
    elif action == '2':
        etl.run_historical(7)
    elif action == '3':
        etl.run_historical(30)
    else:
        print(" Choix invalide")
        return
    
    # Instructions pour vérifier
    print("\n Pour vérifier les données dans PostgreSQL:")
    print("   psql -U postgres -d TP3_DB")
    print("   SELECT * FROM daily_metrics ORDER BY date DESC LIMIT 5;")
    print("   SELECT * FROM channel_metrics ORDER BY date DESC LIMIT 10;")


if __name__ == '__main__':
    main()