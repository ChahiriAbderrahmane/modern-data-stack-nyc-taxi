from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import json

# Seuil d'alerte : 10 000 $ sur le mois en cours
THRESHOLD_REVENUE_AT_RISK = 10000 
SLACK_CONN_ID = 'slack_conn'
POSTGRES_CONN_ID = 'postgres_dbt' 

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_revenue_at_risk(**kwargs):
    """
    Requête la table Gold pour sommer le risque du mois en cours.
    Retourne l'ID de la tâche suivante (alerter ou ne rien faire).
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_dbt')
    
    # SQL : On somme le risque pour l'année et le mois de la date d'exécution du DAG
    # {{ execution_date }} est fourni par Airflow
    sql_query = """
        SELECT SUM(revenue_at_risk_usd)
        FROM gold.data_quality_report
        WHERE pickup_year = EXTRACT(YEAR FROM CAST('{{ ds }}' AS DATE)) - 1
        AND pickup_month = EXTRACT(MONTH FROM CAST('{{ ds }}' AS DATE));
    """
    
    # Exécution de la requête (le rendu Jinja {{ ds }} se fait automatiquement par le hook ou via params)
    exec_date = kwargs['ds'] # Format YYYY-MM-DD
    formatted_sql = sql_query.replace('{{ ds }}', exec_date)
    
    records = pg_hook.get_first(formatted_sql)
    current_risk = records[0] if records and records[0] else 0.0
    
    print(f"💰 Revenue at Risk pour ce mois ({exec_date}) : {current_risk} $")
    
    # Sauvegarde de la valeur pour l'utiliser dans le message Slack
    kwargs['ti'].xcom_push(key='current_risk_amount', value=current_risk)
    
    if float(current_risk) > THRESHOLD_REVENUE_AT_RISK:
        return 'send_slack_alert'
    else:
        return 'everything_is_fine'

with DAG(
    'alerting_revenue_at_risk',
    default_args=default_args,
    description='Alerte Slack si le Revenue at Risk mensuel dépasse 10k$',
    schedule_interval='0 8 * * *', # Tous les jours à 8h00
    catchup=False,
    tags=['quality', 'slack', 'gold']
) as dag:

    start = DummyOperator(task_id='start')

    check_threshold = BranchPythonOperator(
        task_id='check_threshold',
        python_callable=check_revenue_at_risk,
        provide_context=True
    )

    everything_is_fine = DummyOperator(
        task_id='everything_is_fine'
    )

    # 3b. Branche "Alerte Rouge" - Envoi Slack
    # On récupère la valeur via XCom pour l'afficher dans le message
    slack_message = {
        "text": "🚨 *ALERTE QUALITÉ DONNÉES* 🚨\n\n"
                "Le *Revenue at Risk* pour le mois en cours a dépassé le seuil critique.\n"
                "💰 Montant actuel : *{{ ti.xcom_pull(key='current_risk_amount', task_ids='check_threshold') }} $* \n"
                "⚠️ Seuil : 10,000 $\n\n"
                "👉 _Action requise : Vérifier les tables Silver (error_negative_fare, etc.)_"
    }

    send_slack_alert = SimpleHttpOperator(
        task_id='send_slack_alert',
        http_conn_id=SLACK_CONN_ID,
        endpoint='', # Si votre conn contient le webhook complet, laissez vide. Sinon mettez la fin de l'URL.
        method='POST',
        data=json.dumps(slack_message),
        headers={"Content-Type": "application/json"},
    )

    # Orchestration
    start >> check_threshold
    check_threshold >> [everything_is_fine, send_slack_alert]