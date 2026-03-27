import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import io

import telegram

from airflow import DAG

from airflow.decorators import dag, task
from datetime import datetime, timedelta

# ------------------------------------------------------------------------- 

# Параметры DAG 
default_args = {
    'owner': 'n.ignatova',              # владелец DAG
    'depends_on_past': False,          # не зависит от предыдущего запуска
    'retries': 2,                      # количество повторных попыток
    'retry_delay': timedelta(minutes=5),  # задержка между ретраями
    'start_date': datetime(2026, 3, 1),  # дата старта
}

# Расписание запуска (каждый день в 11:00 по мск)
schedule_interval = '0 8 * * *'

# ------------------------------------------------------------------------- 

import os

def load_env(path):
    with open(path) as f:
        for line in f:
            key, value = line.strip().split("=", 1)
            os.environ[key] = value

load_env("AB test/config.env")



#Подключение к БД 
connection = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

# ------------------------------------------------------------------------- 
#Подключаемся к боту
my_token = os.getenv("TELEGRAM_TOKEN")
bot = telegram.Bot(token=my_token)

chat_id = int(os.getenv("CHAT_ID"))

# ------------------------------------------------------------------------- 
# Объявляем DAG 
@dag(
    default_args=default_args,          # передаём параметры
    schedule_interval=schedule_interval, # расписание
    catchup=False                        # не выполнять старые даты
)

   
def ignatova_bot_report():
# -------------------------------------------------------------------------        
    @task()
    def extract_audience():

        #weekly audience
        query = """
        -- подсчитываем отток churn, пользователь был активен previous week, но не пришел в this week
        SELECT 
              this_week, 
              previous_week, 
              -uniq(user_id) as num_users, 
              status 
        FROM
                (SELECT 
                    user_id,
                    groupUniqArray(toMonday(toDate(time))) AS weeks_visited, 
                    addWeeks(arrayJoin(weeks_visited), +1) AS this_week, 
                    if(has(weeks_visited, this_week) = 1, 'retained', 'gone') AS status, 
                    addWeeks(this_week, -1) AS previous_week 
                FROM simulator_20260120.feed_actions
                GROUP BY user_id)
        WHERE status = 'gone' 
              AND this_week >= addWeeks(toMonday(today()), -8)
        GROUP BY this_week, previous_week, status
        HAVING this_week != addWeeks(toMonday(today()), +1) 
        ORDER BY this_week

        UNION ALL

        -- отбираем новых пользователей, также отбираем retained
        SELECT 
              this_week, 
              previous_week, 
              toInt64(uniq(user_id)) as num_users, 
              status 
        FROM
                (SELECT 
                    user_id,
                    groupUniqArray(toMonday(toDate(time))) AS weeks_visited, 
                    arrayJoin(weeks_visited) AS this_week, 
                    if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') AS status, 
                    addWeeks(this_week, -1) AS previous_week 
                FROM simulator_20260120.feed_actions
                GROUP BY user_id)
        WHERE this_week >= addWeeks(toMonday(today()), -8)        
        GROUP BY this_week, previous_week, status
        ORDER BY this_week
         """
        df_audience = ph.read_clickhouse(query, connection=connection)
        df_audience['this_week'] = df_audience['this_week'].dt.date
        df_audience = df_audience.sort_values(by='this_week')
        return df_audience
    
# -------------------------------------------------------------------------    
    @task()
    def extract_dau():

        #dau    

        query = """
        SELECT
            user_id,
            DATE(time) AS day,
            'feed' AS source
        FROM simulator_20260120.feed_actions
        WHERE time < today()
        GROUP BY
            user_id, day

        UNION ALL

        SELECT
            user_id,
            DATE(time) AS day,
            'message' AS source
        FROM simulator_20260120.message_actions
        WHERE time < today()
        GROUP BY
            user_id, day
                """

        df_dau = ph.read_clickhouse(query, connection=connection)
        df_dau = df_dau.groupby(['day', 'source'])['user_id'].count().reset_index()
        return df_dau


# -------------------------------------------------------------------------    
    @task()
    def extract_actions():


        #actions  

        q = """
        SELECT
            day,
            COUNT(DISTINCT user_id) AS unique_users,
            SUM(actions_cnt) AS total_actions,
            total_actions/unique_users AS actions_per_user
        FROM (
            SELECT
                toDate(time) AS day,
                user_id,
                COUNT(*) AS actions_cnt
            FROM simulator_20260120.message_actions
            WHERE toDate(time) < today()
            GROUP BY day, user_id

            UNION ALL

            SELECT
                toDate(time) AS day,
                user_id,
                COUNT(*) AS actions_cnt
            FROM simulator_20260120.feed_actions
            WHERE toDate(time) < today()
            GROUP BY day, user_id
        ) AS combined
        GROUP BY day
        ORDER BY day
        """

        df_actions = ph.read_clickhouse(q, connection=connection)
        return df_actions

# -------------------------------------------------------------------------  

    @task()
    def extract_posts():

        #posts  

        q = """
        SELECT
            toDate(time) AS day,
            COUNT(DISTINCT post_id) AS unique_posts
        FROM simulator_20260120.feed_actions
        WHERE toDate(time) < today()
        GROUP BY day
        ORDER BY day
        """

        df_posts = ph.read_clickhouse(q, connection=connection)
        return df_posts



# -------------------------------------------------------------------------  
    @task()
    def extract_new_users():
    
        #new users

        q = """SELECT 
              this_day, 
              previous_day, 
              COUNT(DISTINCT user_id) AS num_users,
              status,
              source
        FROM
        (
            SELECT 
                user_id,
                groupUniqArray(toDate(time)) AS days_visited, -- массив всех дней посещения
                arrayJoin(days_visited) AS this_day,         -- разворачиваем массив по дням
                if(has(days_visited, addDays(this_day, -1)) = 1, 'retained', 'new') AS status, -- проверка предыдущего дня
                addDays(this_day, -1) AS previous_day,
                source
            FROM simulator_20260120.feed_actions
            GROUP BY user_id, source
        )
        WHERE status = 'new' -- только новые пользователи
        GROUP BY this_day, previous_day, status, source
        ORDER BY this_day
        """
        df_new_users = ph.read_clickhouse(q, connection=connection)
        return df_new_users

# -------------------------------------------------------------------------  
    @task()
    def extract_retention():

    #retention

        q = """
        WITH cohort_size AS (
            SELECT
                start_day,
                countDistinct(user_id) AS cohort_users
            FROM (
                SELECT
                    user_id,
                    min(toDate(time)) AS start_day
                FROM simulator_20260120.feed_actions
                GROUP BY user_id
            )
            GROUP BY start_day
        )

        SELECT
            t1.start_day AS start_day_str,
            t2.date AS date_str,
            countDistinct(t1.user_id) AS active_users,
            cohort_users,
            (countDistinct(t1.user_id) * 100.0 / cohort_users) AS retention_pct  
        FROM
        (
            SELECT
                user_id,
                min(toDate(time)) AS start_day
            FROM simulator_20260120.feed_actions
            GROUP BY user_id

        ) t1
        JOIN
        (
            SELECT DISTINCT
                user_id,
                toDate(time) AS date
            FROM simulator_20260120.feed_actions
        ) t2
        USING user_id
        JOIN cohort_size cs
        ON t1.start_day = cs.start_day

        WHERE 
            t1.start_day >= today() - 7
            AND t1.start_day < today()

        GROUP BY
            t1.start_day,
            t2.date,
            cohort_users

        ORDER BY
            t1.start_day,
            t2.date
        """
        df_retention = ph.read_clickhouse(q, connection=connection)
        # преобразуем строки/даты в datetime
        df_retention['start_day_str'] = pd.to_datetime(df_retention['start_day_str'])
        df_retention['date_str'] = pd.to_datetime(df_retention['date_str'])


        # теперь можно делать dt.date и вычислять разницу
        df_retention['cohort_day'] = (
            df_retention['date_str'] - df_retention['start_day_str']
        ).dt.days

        retention_matrix = df_retention.pivot(
            index='start_day_str',
            columns='cohort_day',
            values='retention_pct'
        )
        return retention_matrix
    
# -------------------------------------------------------------------------  
    @task()
    def create_plot(df_dau, df_new_users, df_actions, df_posts, retention_matrix, df_audience):

        #строим график

        fig, axes = plt.subplots(3, 2, figsize=(15, 10))

        # Daily active users by source
        pivot_dau = df_dau.pivot(index='day', columns='source', values='user_id')
        pivot_dau.plot(kind='line', ax=axes[0, 0])
        axes[0, 0].axhline(0, color='blue', linewidth=1, linestyle='-', alpha=0.7)
        axes[0, 0].set_title('Daily active users')
        axes[0, 0].set_xlabel('Day')
        axes[0, 0].set_ylabel('Users')
        axes[0, 0].legend()
        axes[0, 0].grid(True)

        #трафик новых пользователей
        pivot_new_users = df_new_users.pivot(index='this_day', columns='source', values='num_users')
        pivot_new_users.plot(kind='line', ax=axes[0, 1])
        axes[0, 1].axhline(0, color='blue', linewidth=1, linestyle='-', alpha=0.7)
        axes[0, 1].set_title('New users by source')
        axes[0, 1].set_xlabel('Day')
        axes[0, 1].set_ylabel('Users')
        axes[0, 1].legend()
        axes[0, 1].grid(True)

        # Total actions
        #pivot_dau = df_dau.pivot(index='day', columns='', values='user_id')
        df_actions.plot(kind='line', x='day', y='total_actions', ax=axes[1, 0])
        axes[1, 0].axhline(0, color='blue', linewidth=1, linestyle='-', alpha=0.7)
        axes[1, 0].set_title('Total actions')
        axes[1, 0].set_xlabel('Day')
        axes[1, 0].set_ylabel('Total actions')
        axes[1, 0].legend()
        axes[1, 0].grid(True)

        #Actions per user
        df_posts.plot(kind='line', x='day', y='unique_posts', ax=axes[1, 1])
        axes[1, 1].axhline(0, color='blue', linewidth=1, linestyle='-', alpha=0.7)
        axes[1, 1].set_title('All posts ')
        axes[1, 1].set_xlabel('Day')
        axes[1, 1].set_ylabel('unique posts')
        axes[1, 1].legend()
        axes[1, 1].grid(True)

        #retention
        sns.heatmap(retention_matrix, cmap="RdYlGn", annot=True, fmt=".1f", linewidths=0.5, ax=axes[2, 0], cbar=False)
        axes[2, 0].set_yticklabels(retention_matrix.index.strftime('%Y-%m-%d'))
        axes[2, 0].set_title("Retention rate for last 7 cohorts, %")
        axes[2, 0].set_xlabel('Users lifetime (days)')
        axes[2, 0].set_ylabel('Start cohort day')

        # Weekly audience (stacked bar)
        pivot_audience = df_audience.pivot(index='this_week', columns='status', values='num_users')
        pivot_audience.plot(kind='bar', stacked=True, ax=axes[2, 1])
        axes[2, 1].axhline(0, color='blue', linewidth=1, linestyle='-', alpha=0.7)
        axes[2, 1].set_title('Weekly audience for 2 last month')
        axes[2, 1].set_xlabel('Week')
        axes[2, 1].set_ylabel('Users')
        axes[2, 1].tick_params(axis='x', rotation=45)
        axes[2, 1].legend(loc='lower left', bbox_to_anchor=(0, -0.05), fontsize=10)

        fig.suptitle('Динамика работы приложения', fontsize=12)
        plt.tight_layout()

        plot_object = io.BytesIO() #создаем файловый объект в буфере
        plt.savefig(plot_object, format='png') #сохранили наш график как файл 
        plot_object.seek(0) #передвинули курсор в начало файлового объекта
        plot_object.name = 'test_plot.png'
        plt.close(fig)

        return plot_object
# -------------------------------------------------------------------------  
    @task()
    def extract_feed_metrics():

        # метрики за вчера по ленте
        query = """
        SELECT 
            toDate(time) AS day,
            COUNT (DISTINCT user_id) AS dau,
            sum(action = 'view') AS views,
            sum(action = 'like') AS likes,
            sum(action = 'like')/sum(action = 'view') AS ctr
        FROM simulator_20260120.feed_actions
        WHERE day = today()-1
        GROUP BY day
                """
        feed_metrics = ph.read_clickhouse(query, connection=connection)
        
        return feed_metrics
    
    @task()
    def extract_msg_metrics():

        # метрики за вчера по мессенджеру
        query = """
        SELECT
            DATE(time) AS day,
            COUNT(DISTINCT user_id) as dau,
            COUNT (receiver_id) as msgs_sent

        FROM simulator_20260120.message_actions
        WHERE day = today()-1
        GROUP BY day
        """
        msg_metrics = ph.read_clickhouse(query, connection=connection)
    
        return  msg_metrics

# -------------------------------------------------------------------------  
    @task()
    def create_msg(feed_metrics, msg_metrics):
        
        # Формируем сообщение
        message_text = (
            f"Отчет по приложению:\n"
            f"Метрики по новостной ленте за {feed_metrics.day.iloc[0].strftime('%d-%m-%Y')}:\n"
            f"DAU: {(f'{feed_metrics.dau.iloc[0]:,}').replace(',', ' ')}\n"
            f"Views: {(f'{feed_metrics.views.iloc[0]:,}').replace(',', ' ')}\n"
            f"Likes: {(f'{feed_metrics.likes.iloc[0]:,}').replace(',', ' ')}\n"
            f"CTR: {feed_metrics.ctr.iloc[0]:.2%}\n\n"

            f"Метрики по мессенджеру за {msg_metrics.day.iloc[0].strftime('%d-%m-%Y')}:\n"
            f"DAU: {(f'{msg_metrics.dau.iloc[0]:,}').replace(',', ' ')}\n"
            f"Msgs sent: {(f'{msg_metrics.msgs_sent.iloc[0]:,}').replace(',', ' ')}"
                )
        
        return message_text

# -------------------------------------------------------------------------  
    @task()
    def send_msg(plot_object, message_text):

        #Отправляем сообщение
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption = message_text)
        
        
    #связываем таски
    df_audience = extract_audience()
    df_dau = extract_dau()
    df_actions = extract_actions()
    df_posts = extract_posts()
    df_new_users = extract_new_users()
    retention_matrix = extract_retention()
    plot_object = create_plot(df_dau, df_new_users, df_actions, df_posts, retention_matrix, df_audience)
    feed_metrics = extract_feed_metrics()
    msg_metrics = extract_msg_metrics()
    message_text = create_msg(feed_metrics, msg_metrics)
    send_msg(plot_object, message_text)
    
    
       
# Инициализация DAG
ignatova_bot_report = ignatova_bot_report()