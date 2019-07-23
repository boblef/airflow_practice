import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Define cvs file path
csv_path = Path("../data/index.csv")

# List of words that script should avoid to write onto csv.
avoid_words = ["My Local",
               "Editor's Blog",
               "Connect with CBC",
               "Contact CBC",
               "Services & Info",
               "Accessibility"]

# Define URL where scrape from.
cbc_url = "https://www.cbc.ca/news/canada"

# Default args used when create a new dag
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 18),
    # 'end_date': datetime(2018, 12, 30),
    'depends_on_past': False,
    'email': ['kohei.suzuki808@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}


# Create a new dag
dag = DAG(
    'hw2',
    default_args=default_args,
    description='Homework2',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)


# Define task1
task1 = BashOperator(
    task_id="echo1",
    bash_command="echo Start scraping.",
    dag=dag,
)


def get_news_from_cbc(url):
    """
    Scrape headlines of news on https://www.cbc.ca/news/canada.
    Then write them into a csv file with additional information
    such as time and url.

    Parameters
    ----------
    url: str
    A URL to a website where I want to scrape from

    Returns
    -------
    current_top_news: list
    This is a list, and each element is also a list that contains
    1. headline
    2. source url
    3. date and time

    Example:
    [["headline1", "url", datetime],
     ["headline2", "url", datetime]]

    Where "headline" and "url" are string, datetime is datetime object.
    """
    response = requests.get(url)
    response.encoding = response.apparent_encoding

    bs = BeautifulSoup(response.text, 'html.parser')

    datetime_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    current_top_news = []
    for i in bs.select("h3"):
        if not i.getText() in avoid_words:
            print(i.getText())  # Print each headline.
            current_top_news.append(list((i.getText(), url, datetime_now)))
        else:
            continue

    return current_top_news


# Define task2
task2 = PythonOperator(
    task_id="scraping",
    python_callable=get_news_from_cbc,
    op_kwargs={'url': cbc_url},
    dag=dag)


def write_csv(**kwargs):
    """
    Write information in the given list into a csv file.
    To get the list we just created, we use Xcoms.

    Returns
    -------
    Boolean: True or False

    When we success writing things correctly, returns True. Otherwise False.
    """

    # Xcoms to get the list
    ti = kwargs['ti']
    current_top_news = ti.xcom_pull(task_ids='scraping')

    try:
        with open(str(csv_path), "a") as file:
            writer = csv.writer(file, lineterminator='\n')
            writer.writerows(current_top_news)
        return True
    except OSError as e:
        print(e)
        return False


task3 = PythonOperator(
    task_id="writing_csv",
    python_callable=write_csv,
    provide_context=True,
    dag=dag)


def confirmation(**kwargs):
    """
    If everything is done properly, print "Done!!!!!!"
    Otherwise print "Failed."
    """

    # Xcoms to get status which is the return value of write_csv().
    ti = kwargs['ti']
    status = ti.xcom_pull(task_ids='writing_csv')

    if status:
        print("Done!!!!!!")
    else:
        print("Failed.")


task4 = PythonOperator(
    task_id="confirmation",
    python_callable=confirmation,
    provide_context=True,
    dag=dag
)


task1 >> task2 >> task3 >> task4
