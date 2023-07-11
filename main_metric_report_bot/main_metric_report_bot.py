import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # Так как мы пишем таски в питоне
from datetime import date, timedelta, datetime

# задаем подключение к базе данных для выгрузки данных
connection = {'host': '<скрыто в целях безопасности>'
    , 'database': '<скрыто в целях безопасности>'
    , 'user': '<скрыто в целях безопасности>'
    , 'password': '<скрыто в целях безопасности>'
              }


# функция для выгрузки необходимых данных
def get_df(query, connection=connection):
    df_ext = ph.read_clickhouse(query, connection=connection)
    return df_ext


# выгружаем нужные данные из кликхаус
# на всякий случай ссылка на запрос в редаш - https://redash.lab.karpov.courses/queries/33751/source
df = '''
        select  dt,
                dau,
                views,
                likes,
                ctr,
                prev_dau,
                prev_views,
                prev_likes,
                prev_ctr,
                dau - prev_dau as dev_dau, --отклонение от предыдущей даты
                views - prev_views as dev_views,
                likes - prev_likes as dev_likes,
                ctr - prev_ctr as dev_ctr
        from    
            (select dt,
                    dau,
                    views,
                    likes,
                    ctr,
                    lagInFrame(dau) over(rows between unbounded preceding and unbounded following)  as prev_dau, -- значение от предыдущего дня
                    lagInFrame(views) over(rows between unbounded preceding and unbounded following) as prev_views,
                    lagInFrame(likes) over(rows between unbounded preceding and unbounded following) as prev_likes,
                    lagInFrame(ctr) over(rows between unbounded preceding and unbounded following)  as prev_ctr
            from 
                    (select  toDate(time) as dt,
                            count(distinct user_id) as dau,
                            countIf(action = 'view') as views,
                            countIf(action = 'like') as likes,
                            round((countIf(action = 'like') / countIf(action = 'view'))*100, 3) as ctr
                    from    simulator_20230620.feed_actions
                    group by dt
                    order by dt) as t1) as t2
        where dt > today()-8 and dt != today()
        order by dt desc
      '''
df = get_df(query=df, connection=connection)

# задаем переменную, где будет находится день за который предоставлен отчет
report_day = (date.today() - timedelta(days=1)).strftime('%d-%m-%Y')

# задаем дефолтные параметры DAG
default_args = {
    'owner': 'k-shutova',  # создатель DAG
    'depends_on_past': False,
    # зависимость от запусков, False - не зависеть, запускать дальше даже если была ошибка в предыдущем запуске
    'retries': 2,  # число попыток выполнить DAG если произошел сбой
    'retry_delay': timedelta(minutes=5),  # временной промежуток между перезапусками
    'start_date': datetime(2023, 7, 7),  # дата начала выполнения DAG
}

# задаем интервал выполнения DAG - каждый день в 11:00
schedule_interval = '59 10 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def shutova_report_bot_one():
    @task
    def metrics_report(chat=None):
        chat_id = chat or <здесь был указан id чата скрыт в целях безопасности>

        bot_token = '<здесь был указан токен бота скрыт в целях безопасности>'
        bot = telegram.Bot(token=bot_token)

        # формируем сообщение, которое будет падать в чат
        text = 'Добрый день!\n\n' \
               'Спешу направить Вам отчет по ключевым метрикам за {}:\n\n' \
               'DAU: {}\n' \
               'Значение предыдущего дня: {}\n' \
               'Отклонение предыдущего дня: {}\n\n' \
               'Значение предыдущего дня: {}\n' \
               'Отклонение от предыдущего дня: {}\n\n' \
               'Likes: {}\n' \
               'Значение предыдущего дня: {}\n' \
               'Отклонение от предыдущего дня: {}\n\n' \
               'CTR (в %): {}\n' \
               'Значение предыдущего дня: {}\n' \
               'Отклонение от предыдущего дня: {}'.format(report_day
                                                          , df.dau[0]
                                                          , df.prev_dau[0]
                                                          , df.dev_dau[0]
                                                          , df.views[0]
                                                          , df.prev_views[0]
                                                          , df.dev_views[0]
                                                          , df.likes[0]
                                                          , df.prev_likes[0]
                                                          , df.dev_likes[0]
                                                          , df.ctr[0]
                                                          , df.prev_ctr[0]
                                                          , df.dev_ctr[0].round(3))

        # отправляем текстовое сообщение
        bot.sendMessage(chat_id=chat_id, text=text)

        # настраиваем графики, которые будем отправлять в сообещнии
        # направляем линейные графики за 7 дней со значениями DAU, views, likes, CTR

        # график DAU
        sns.set(style='darkgrid')  # чтобы графики были красивые
        plt.figure(figsize=(15, 5))  # задаем размер графиков
        sns.lineplot(data=df, x='dt', y='dau', color='red')
        plt.title('DAU за предыдущие 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Значение DAU')

        # отправка графика
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'dau_plot.png'
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        # график лайков и просмотров
        sns.set(style='darkgrid')  # чтобы графики были красивые
        plt.figure(figsize=(15, 5))  # задаем размер графиков
        sns.lineplot(data=df, x='dt', y='views', color='green', label='Просмотры')
        sns.lineplot(data=df, x='dt', y='likes', color='blue', label='Лайки')
        plt.title('Лайки и просмотры за предыдущие 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Значение')

        # отправка графика
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'views_likes_plot.png'
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        # график CTR
        sns.set(style='darkgrid')  # чтобы графики были красивые
        plt.figure(figsize=(15, 5))  # задаем размер графиков
        sns.lineplot(data=df, x='dt', y='ctr', color='orange')
        plt.title('CTR в % за предыдущие 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Значение CTR')

        # отправка графика
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'ctr_plot.png'
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    metrics_report()


shutova_report_bot_one = shutova_report_bot_one()