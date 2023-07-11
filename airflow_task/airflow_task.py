# импорт необходимых библиотек
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import numpy as np
from io import StringIO
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # Так как мы пишем таски в питоне

# задаем подключение к базе данных для выгрузки данных
connection_extraction = {'host': '<скрыто в целях безопасности>'
    , 'database': '<скрыто в целях безопасности>'
    , 'user': '<скрыто в целях безопасности>'
    , 'password': '<скрыто в целях безопасности>'
                         }

# задаем подключение к базе данных для загрузки данных
connection_loading = {'host': '<скрыто в целях безопасности>'
    , 'database': 'test'
    , 'user': '<скрыто в целях безопасности>'
    , 'password': '<скрыто в целях безопасности>'
                      }


# функция для выгрузки необходимых данных
def get_df(query, connection=connection_extraction):
    df_ext = ph.read_clickhouse(query, connection=connection_extraction)
    return df_ext


# задаем дефолтные параметры
default_args = {
    'owner': 'k-shutova',  # создатель DAG
    'depends_on_past': False,
    # зависимость от запусков, False - не зависеть, запускать дальше даже если была ошибка в предыдущем запуске
    'retries': 2,  # число попыток выполнить DAG если произошел сбой
    'retry_delay': timedelta(minutes=5),  # временной промежуток между перезапусками
    'start_date': datetime(2023, 7, 5),  # дата начала выполнения DAG
}

# задаем интервал выполнения DAG - каждый день в 12:00
schedule_interval = '0 12 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval,
     catchup=False)  # catchup=False - не считает прошлое, только текущий день
def shutova_task_one():
    # выгружаем данные из feed_actions
    # на всякий случай ссылка на рабочий заброс в редаш - https://redash.lab.karpov.courses/queries/33443/source
    @task
    def extract_feed():
        feed_df = '''
                    SELECT  DISTINCT toDate(time) AS event_date,  
                            user_id,
                            countIf(action='like') as likes,
                            countIf(action='view') as views,
                            os,
                            if(gender=0, 'male', 'female') as gender,
                            age
                    FROM    simulator_20230620.feed_actions
                    WHERE   toDate(time) == today() - 1
                    GROUP BY event_date, user_id, user_id, os, gender, age
                '''
        feed_df = get_df(query=feed_df, connection=connection_extraction)
        return feed_df

    # выгружаем данные из message_action
    # на всякий случай ссылка на рабочий заброс в редаш - https://redash.lab.karpov.courses/queries/33444/source
    @task
    def extract_messages():
        mess_df = '''
                    SELECT  if(true, toDate(today()-1), '') as event_date,
                            user_id,
                            messages_sent,
                            users_sent,
                            messages_received,
                            users_received,
                            os,
                            gender,
                            age
                    FROM    (
                            SELECT  user_id,
                                    count(reciever_id) as messages_sent,
                                    count(distinct reciever_id) as users_sent,
                                    os,
                                    age,
                                    if(gender=0, 'male', 'female') as gender
                            FROM    simulator_20230620.message_actions
                            WHERE   toDate(time) == today() - 1
                            GROUP BY user_id, os, age, gender
                            ) AS t1
                    JOIN    (
                            SELECT  reciever_id as user_id,
                                    count(user_id) as messages_received,
                                    count(distinct user_id) as users_received
                            FROM    simulator_20230620.message_actions
                            WHERE   toDate(time) == today() - 1
                            GROUP BY reciever_id
                            ) AS t2 
                    USING (user_id)
                  '''
        mess_df = get_df(query=mess_df, connection=connection_extraction)
        return mess_df

    # выполняем объединение выгруженных таблиц
    @task
    def merge_df(feed_df, mess_df):
        main_df = feed_df.merge(mess_df
                                , how='outer'
                                , on=['event_date', 'user_id', 'gender', 'age', 'os']).fillna(0)
        return main_df

    # выполняем срез по полу
    @task
    def dimension_gender(main_df):
        gender_df = main_df.groupby(by=['event_date', 'gender'], as_index=False)[['views'
            , 'likes'
            , 'messages_sent'
            , 'users_sent'
            , 'messages_received'
            , 'users_received']].sum()
        gender_df['dimensions'] = 'gender'
        gender_df.rename(columns={'gender': 'dimensions_value'}, inplace=True)
        gender_df = gender_df[['event_date'
            , 'dimensions'
            , 'dimensions_value'
            , 'views'
            , 'likes'
            , 'messages_sent'
            , 'messages_received'
            , 'users_sent'
            , 'users_received']]

        return gender_df

    # выполняем срез по возрасту
    @task
    def dimension_age(main_df):
        age_df = main_df.groupby(by=['event_date', 'age'], as_index=False)[['views'
            , 'likes'
            , 'messages_sent'
            , 'users_sent'
            , 'messages_received'
            , 'users_received']].sum()
        age_df['dimensions'] = 'age'
        age_df.rename(columns={'age': 'dimensions_value'}, inplace=True)
        age_df = age_df[['event_date'
            , 'dimensions'
            , 'dimensions_value'
            , 'views'
            , 'likes'
            , 'messages_sent'
            , 'messages_received'
            , 'users_sent'
            , 'users_received']]

        return age_df

    # выполняем срез по операционной системе
    @task
    def dimension_os(main_df):
        os_df = main_df.groupby(by=['event_date', 'os'], as_index=False)[['views'
            , 'likes'
            , 'messages_sent'
            , 'users_sent'
            , 'messages_received'
            , 'users_received']].sum()
        os_df['dimensions'] = 'os'
        os_df.rename(columns={'os': 'dimensions_value'}, inplace=True)
        os_df = os_df[['event_date'
            , 'dimensions'
            , 'dimensions_value'
            , 'views'
            , 'likes'
            , 'messages_sent'
            , 'messages_received'
            , 'users_sent'
            , 'users_received']]

        return os_df

    # выполняем объединение таблиц со срезами
    @task
    def merge_dimensions(gender_df, age_df, os_df):
        dimensions_df = pd.concat([gender_df, age_df, os_df], axis=0)
        dimensions_df[['views'
            , 'likes'
            , 'messages_sent'
            , 'users_sent'
            , 'messages_received'
            , 'users_received']] = dimensions_df[['views'
            , 'likes'
            , 'messages_sent'
            , 'users_sent'
            , 'messages_received'
            , 'users_received']].astype(int)

        # "сортирую" колонки в том порядке, в котором того требует поставленная задача
        dimensions_df = dimensions_df[['event_date'
            , 'dimensions'
            , 'dimensions_value'
            , 'views'
            , 'likes'
            , 'messages_received'
            , 'messages_sent'
            , 'users_received'
            , 'users_sent']]
        return dimensions_df

    # создаем таблицу со срезами и загружаем ее в схему test
    @task
    def loading(dimensions_df):
        load_df = '''
                    CREATE TABLE IF NOT EXISTS test.shutova_final_df
                    (
                     event_date Date,
                     dimensions VARCHAR(30),
                     dimensions_value VARCHAR(30),
                     views Int64,
                     likes Int64,
                     messages_received Int64,
                     messages_sent Int64,
                     users_received Int64,
                     users_sent Int64
                     ) 
                     ENGINE = MergeTree()
                     order by event_date
                 '''
        ph.execute(query=load_df, connection=connection_loading)
        ph.to_clickhouse(df=dimensions_df, table='shutova_final_df', connection=connection_loading, index=False)

        # выполняем таски

    feed_df = extract_feed()
    mess_df = extract_messages()
    main_df = merge_df(feed_df, mess_df)
    gender_df = dimension_gender(main_df)
    age_df = dimension_age(main_df)
    os_df = dimension_os(main_df)
    dimensions_df = merge_dimensions(gender_df, age_df, os_df)
    loading(dimensions_df)


shutova_task_one = shutova_task_one()