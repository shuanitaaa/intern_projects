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


def get_df(query, connection=connection):
    df_ext = ph.read_clickhouse(query, connection=connection)
    return df_ext


# выгружаем нужные данные из кликхаус
# на всякий случай ссылка на запрос в редаш - https://redash.lab.karpov.courses/queries/33836/source
df = '''
        with t1 as (SELECT  count(DISTINCT user_id) AS feed_users,
                            toDate(time) as dt
                    FROM
                      (SELECT *
                       FROM     simulator_20230620.feed_actions
                       WHERE    user_id NOT IN (SELECT DISTINCT user_id
                                                FROM simulator_20230620.message_actions))
                    group by dt),

        t2 as       (select toDate(time) as dt,
                            count(distinct user_id) as dau,
                            countIf(action = 'view') as views,
                            countIf(action = 'like') as likes,
                            count(distinct post_id) as post_count,
                            round((countIf(action = 'like') / countIf(action = 'view'))*100, 3) as ctr
                    from    simulator_20230620.feed_actions
                    group by dt
                    order by dt),

        t3 as       (SELECT count(DISTINCT user_id) AS all_app_users,
                            toDate(time) as dt
                    FROM
                          (SELECT *
                           FROM simulator_20230620.feed_actions
                           WHERE user_id IN (SELECT DISTINCT user_id
                                             FROM simulator_20230620.message_actions))
                    group by dt),

        t4 as       (select toDate(time) as dt,
                            count(distinct user_id) as dau,
                            count(reciever_id) as messages_count
                    from    simulator_20230620.message_actions
                    group by dt
                    order by dt),

        t5 as       (select     countIf(distinct user_id, os = 'iOS') as feed_ios_users,
                                countIf(distinct user_id, os = 'Android') as feed_android_users,
                                toDate(time) as dt
                    from        simulator_20230620.feed_actions
                    group by dt),

        t8 as       (select     countIf(distinct user_id, os = 'iOS') as mess_ios_users,
                                countIf(distinct user_id, os = 'Android') as mess_android_users,
                                toDate(time) as dt
                    from        simulator_20230620.message_actions
                    group by dt),

        t6 as       (select     countIf(distinct user_id, source = 'ads') as feed_ads_users,
                                countIf(distinct user_id, source = 'organic') as feed_organic_users,
                                toDate(time) as dt
                    from        simulator_20230620.feed_actions
                    group by dt),

        t7 as       (select     countIf(distinct user_id, source = 'ads') as mess_ads_users,
                                countIf(distinct user_id, source = 'organic') as mess_organic_users,
                                toDate(time) as dt
                    from        simulator_20230620.message_actions
                    group by dt)

        select  t2.dt as dt,
                t2.dau as feed_dau,
                t4.dau as mess_dau,
                t2.post_count as post_count,
                t4.messages_count as messages_count,
                t1.feed_users as feed_users,
                t5.feed_ios_users as feed_ios_users,
                t5.feed_android_users as feed_android_users,
                t8.mess_ios_users as mess_ios_users,
                t8.mess_android_users as mess_android_users,
                t3.all_app_users as all_app_users,
                t2.views as views,
                t2.likes as likes,
                t2.ctr as ctr,
                t6.feed_ads_users as feed_ads_users,
                t6.feed_organic_users as feed_organic_users,
                t7.mess_ads_users as mess_ads_users,
                t7.mess_organic_users as mess_organic_users
        from t2
        JOIN t1 on t2.dt = t1.dt
        JOIN t3 on t2.dt = t3.dt
        JOIN t4 on t2.dt = t4.dt
        JOIN t5 on t2.dt = t5.dt
        JOIN t6 on t2.dt = t6.dt
        JOIN t7 on t2.dt = t7.dt
        JOIN t8 on t2.dt = t8.dt
        where t2.dt > today()-8 and t2.dt != today()
        order by t2.dt desc
      '''

df = get_df(query=df, connection=connection)

feed_trafic = '''WITH ios as    (select toDate(time) as dt,
                                        countIf(action = 'view') as ios_views,
                                        countIf(action = 'like') as ios_likes
                                from    simulator_20230620.feed_actions
                                where   os = 'iOS'
                                group by dt),

                android as      (select toDate(time) as dt,
                                        countIf(action = 'view') as android_views,
                                        countIf(action = 'like') as android_likes
                                from    simulator_20230620.feed_actions
                                where   os = 'Android'
                                group by dt),

                ads as          (select toDate(time) as dt,
                                        countIf(action = 'view') as ads_views,
                                        countIf(action = 'like') as ads_likes
                                from    simulator_20230620.feed_actions
                                where   source = 'ads'
                                group by dt),

                organic as      (select toDate(time) as dt,
                                        countIf(action = 'view') as organic_views,
                                        countIf(action = 'like') as organic_likes
                                from    simulator_20230620.feed_actions
                                where   source = 'organic'
                                group by dt),

                date as         (select distinct toDate(time) as dt
                                from    simulator_20230620.feed_actions)

                select  date.dt as dt,
                        ios_views,
                        ios_likes,
                        android_views,
                        android_likes,
                        ads_views,
                        ads_likes,
                        organic_views,
                        organic_likes
                from    date
                join    ios on date.dt = ios.dt
                join    android on date.dt = android.dt
                join    ads on date.dt = ads.dt
                join    organic on date.dt = organic.dt
                where date.dt > today()-8 and date.dt != today()
                order by date.dt desc
                '''

# таблица для графика
feed_trafic = get_df(query=feed_trafic, connection=connection)

mess_trafic = '''
                WITH ios as (select toDate(time) as dt,
                                    count(reciever_id) as ios_mess_count
                            from    simulator_20230620.message_actions
                            where   os = 'iOS'
                            group by dt),

                android as      (select toDate(time) as dt,
                                        count(reciever_id) as android_mess_count
                                from    simulator_20230620.message_actions
                                where   os = 'Android'
                                group by dt),

                ads as          (select toDate(time) as dt,
                                        count(reciever_id) as ads_mess_count
                                from    simulator_20230620.message_actions
                                where   source = 'ads'
                                group by dt),

                organic as      (select toDate(time) as dt,
                                        count(reciever_id) as organic_mess_count
                                from    simulator_20230620.message_actions
                                where   source = 'organic'
                                group by dt),

                date as         (select distinct toDate(time) as dt
                                from    simulator_20230620.message_actions)

                select  date.dt as dt,
                        ios_mess_count,
                        android_mess_count,
                        ads_mess_count,
                        organic_mess_count
                from    date
                join    ios on date.dt = ios.dt
                join    android on date.dt = android.dt
                join    ads on date.dt = ads.dt
                join    organic on date.dt = organic.dt
                where date.dt > today()-8 and date.dt != today()
                order by date.dt desc      
                '''
# еще одна таблица для графика
mess_trafic = get_df(query=mess_trafic, connection=connection)

# задаем переменную, где будет находится день за который предоставлен отчет
report_day = (date.today() - timedelta(days=1)).strftime('%d-%m-%Y')

# задаем дефолтные параметры DAG
default_args = {
    'owner': 'k-shutova',  # создатель DAG
    'depends_on_past': False,
    # зависимость от запусков, False - не зависеть, запускать дальше даже если была ошибка в предыдущем запуске
    'retries': 2,  # число попыток выполнить DAG если произошел сбой
    'retry_delay': timedelta(minutes=5),  # временной промежуток между перезапусками
    'start_date': datetime(2023, 7, 8),  # дата начала выполнения DAG
}

# задаем интервал выполнения DAG - каждый день в 11:00, поставила в расписании время на 3 минуты раньше, в качестве лага для обработки
# и чтобы не пересекаться с чужими сообщениями в чате
schedule_interval = '57 10 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def shutova_report_bot_two():
    @task
    def full_report(chat=None):
        chat_id = chat or '<скрыто в целях безопасности>'

        bot_token = '<скрыто в целях безопасности>'
        bot = telegram.Bot(token=bot_token)

        # формируем сообщение, которое будет падать в чат
        text = 'Добрый день!\n\n' \
               'Направляю Вам общий отчет по приложению за {}:\n\n' \
               '*Ключевые метрики:*\n' \
               'DAU новостной ленты: {}\n' \
               'DAU мессенджера: {}\n' \
               'CTR новостной ленты (в %): {}\n' \
               'Views: {}\n' \
               'Likes: {}\n' \
               'Число уникальных постов: {}\n' \
               'Число отправленных сообщений: {}\n\n' \
               '*Среди пользователей новостной ленты:*\n' \
               'Используют только новостную ленту {} пользователей\n' \
               'Используют оба сервиса {} пользователей\n\n' \
               '*Все о рекламных пользователях:*\n' \
               'Уникальные пользователи ленты: {}\n' \
               'Уникальные пользователи мессенджера: {}\n\n' \
               '*Все об органических пользователях:*\n' \
               'Уникальные пользователи ленты: {}\n' \
               'Уникальные пользователи мессенджера: {}\n\n' \
               '*Активность пользователей разных ОС:*\n' \
               'Уникальные iOS-пользователи ленты: {}\n' \
               'Уникальные Android-пользователи ленты: {}\n' \
               'Уникальные iOS-пользователи мессенджера: {}\n' \
               'Уникальные Android-пользователи мессенджера: {}'.format(report_day,
                                                                        df.feed_dau[0],
                                                                        df.mess_dau[0],
                                                                        df.ctr[0],
                                                                        df.views[0],
                                                                        df.likes[0],
                                                                        df.post_count[0],
                                                                        df.messages_count[0],
                                                                        df.feed_users[0],
                                                                        df.all_app_users[0],
                                                                        df.feed_ads_users[0],
                                                                        df.mess_ads_users[0],
                                                                        df.feed_organic_users[0],
                                                                        df.mess_organic_users[0],
                                                                        df.feed_ios_users[0],
                                                                        df.feed_ios_users[0],
                                                                        df.mess_ios_users[0],
                                                                        df.mess_ios_users[0])

        # отправляем текстовое сообщение
        bot.sendMessage(chat_id=chat_id, text=text, parse_mode='Markdown')

        # настраиваем графики, которые будем отправлять в сообещнии
        # направляем линейные графики за 7 дней

        # график DAU
        sns.set(style='darkgrid')  # чтобы графики были красивые:)
        plt.figure(figsize=(12, 7))  # задаем размер графиков

        plt.subplot(1, 2, 1)
        sns.lineplot(data=df, x='dt', y='feed_dau', color='red')
        plt.xticks(rotation=45)
        plt.title('DAU новостной ленты за предыдущие 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Значение DAU новостной ленты')

        plt.subplot(1, 2, 2)
        sns.lineplot(data=df, x='dt', y='mess_dau', color='green')
        plt.xticks(rotation=45)
        plt.title('DAU мессенджера за предыдущие 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Значение DAU мессенджера')

        # отправка графиков
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'dau_plot.png'
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        # график CTR
        sns.set(style='darkgrid')  # чтобы графики были красивые:)
        plt.figure(figsize=(15, 5))  # задаем размер графиков
        sns.lineplot(data=df, x='dt', y='ctr', color='orange')
        plt.title('CTR в % за предыдущие 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Значение CTR')

        # график Просмотров и лайков - ГОТОВ
        sns.set(style='darkgrid')  # чтобы графики были красивые:)
        plt.figure(figsize=(15, 10))  # задаем размер графиков
        plt.subplots_adjust(left=0.1,
                            bottom=0.1,
                            right=0.9,
                            top=0.9,
                            wspace=0.4,
                            hspace=0.4)

        ax1 = plt.subplot(311)
        sns.lineplot(data=df, x='dt', y='views', color='red', label='Просмотры')
        sns.lineplot(data=df, x='dt', y='likes', color='blue', label='Лайки')
        plt.title('Просмотры и лайки за предыдущие 7 дней')
        plt.xlabel(' ')
        plt.ylabel(' ')

        # график просмотров и лайков по операционным системам
        ax2 = plt.subplot(312, sharex=ax1)
        sns.lineplot(data=feed_trafic, x='dt', y='ios_views', color='red', label='Просмотры iOS')
        sns.lineplot(data=feed_trafic, x='dt', y='ios_likes', color='blue', label='Лайки iOS')
        sns.lineplot(data=feed_trafic, x='dt', y='android_views', color='green', label='Просмотры Android')
        sns.lineplot(data=feed_trafic, x='dt', y='android_likes', color='black', label='Лайки Android')
        plt.title('Просмотры и лайки за предыдущие 7 дней в разрезе операционных систем')
        plt.xlabel(' ')
        plt.ylabel(' ')

        # # график просмотров и лайков по каналам привлечения
        ax3 = plt.subplot(313, sharex=ax1, sharey=ax1)
        sns.lineplot(data=feed_trafic, x='dt', y='ads_views', color='red', label='Рекламные просмотры')
        sns.lineplot(data=feed_trafic, x='dt', y='ads_likes', color='blue', label='Рекламные лайки')
        sns.lineplot(data=feed_trafic, x='dt', y='organic_views', color='green', label='Органические просмотры')
        sns.lineplot(data=feed_trafic, x='dt', y='organic_likes', color='black', label='Органические лайки')
        plt.title('Просмотры и лайки за предыдущие 7 дней в разрезе канала привлечения')
        plt.xlabel(' ')
        plt.ylabel(' ');

        # отправка графиков
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'likes_views_plot.png'
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        # график по числу сообщений
        sns.set(style='darkgrid')  # чтобы графики были красивые:)
        plt.figure(figsize=(15, 10))  # задаем размер графиков
        plt.subplots_adjust(left=0.1,
                            bottom=0.1,
                            right=0.9,
                            top=0.9,
                            wspace=0.4,
                            hspace=0.4)

        ax1 = plt.subplot(311)
        sns.lineplot(data=df, x='dt', y='messages_count', color='red', label='Просмотры')
        plt.title('Отправленные сообщения за предыдущие 7 дней')
        plt.xlabel(' ')
        plt.ylabel(' ')

        # график отправленных сообщений в разрезе операционных систем
        ax2 = plt.subplot(312, sharex=ax1)
        sns.lineplot(data=mess_trafic, x='dt', y='ios_mess_count', color='red', label='Сообщения iOS')
        sns.lineplot(data=mess_trafic, x='dt', y='android_mess_count', color='blue', label='Сообщения Android')
        plt.title('Отправленные сообщения 7 дней в разрезе операционных систем')
        plt.xlabel(' ')
        plt.ylabel(' ')

        # график отправленных сообщений в разрезе канала привлечения
        ax3 = plt.subplot(313, sharex=ax1, sharey=ax1)
        sns.lineplot(data=mess_trafic, x='dt', y='ads_mess_count', color='red', label='Рекламные сообщения')
        sns.lineplot(data=mess_trafic, x='dt', y='organic_mess_count', color='green', label='Органические сообщения')
        plt.title('Отправленные сообщения 7 дней в разрезе канала привлечения')
        plt.xlabel(' ')
        plt.ylabel(' ');

        # отправка графиков
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'messages_plot.png'
        plot_object.seek(0)
        plt.close

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    full_report()


shutova_report_bot_two = shutova_report_bot_two()
