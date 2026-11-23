# Спринт 7. Организация Data Lake

Проект строит три витрины для геоаналитики и рекомендации друзей. Расчёты выполняются в PySpark, ежедневный запуск организует Airflow.

## Структура Data Lake

| Путь в HDFS | Назначение | Формат и обновление |
| --- | --- | --- |
| `/user/master/data/geo/events/date=YYYY-MM-DD` | Исходный слой событий | Parquet, дневные партиции поставщика данных |
| `/user/master/data/geo/sprint7/reference/geo.csv` | Справочник координат центров городов | CSV с разделителем `;`, обновляется при изменении справочника |
| `/user/master/data/geo/sprint7/test/{users,zones,friends}/date=YYYY-MM-DD` | Проверка на выбранном диапазоне дат | Parquet, по запросу |
| `/user/master/data/geo/sprint7/analytics/{users,zones,friends}/date=YYYY-MM-DD` | Снимки витрин для аналитиков | Parquet, ежедневно в 04:00 UTC на конец предыдущих суток UTC |

Папка `date` обозначает дату снимка. Для выбранной витрины и даты повторный запуск заменяет соответствующую партицию. История нужна для поиска первой активности, долгих визитов и прежней переписки. В обычном запуске используется вся доступная история до даты расчёта включительно. На статическом учебном датасете дату зафиксируйте вручную при запуске DAG по последней партиции источника.

## Правила расчёта

- **Пользователи.** Сообщение с координатами относится к ближайшему центру города по формуле гаверсинуса с радиусом 6371 км. `act_city` и `local_time` берутся по последнему сообщению с известной позицией. Визит продолжается до первого сообщения из другого города. Домашним считается город последнего визита, в котором между первым и последним местными календарными днями прошло больше 27 дней. Допускаются дни без сообщений, поскольку отсутствие сообщения само по себе не свидетельствует о смене города. Если такого визита не было, `home_city` равен `null`. Первое пребывание и каждый возврат учитываются в `travel_count` и `travel_array`.
- **Зоны.** У сообщений используется их собственная позиция. Реакции, подписки и условные регистрации привязываются к последнему известному городу пользователя на дату снимка. Дата регистрации определяется по времени первого исходящего сообщения пользователя, включая сообщения без координат. Событие попадает в геослой при наличии города. `month` — первое число месяца, `week` — понедельник, границы периодов идут по UTC. Полный недельный итог повторяется, если неделя представлена в двух месяцах. Месячный итог повторяется в строках недель этого месяца. Для сводки выбирайте по одному итогу на пару «период, зона».
- **Друзья.** Координаты каждого участника берутся из последнего сообщения с известным местоположением. Пара должна иметь общий канал и находиться на расстоянии до 1 км включительно. Из кандидатов исключается переписка в обе стороны, включая сообщения без координат. Каждая пара хранится один раз с `user_left < user_right`. `zone_id` и `local_time` относятся к последнему геопривязанному сообщению `user_left`, `processed_dttm` отражает момент расчёта.
- **Часовые пояса.** Сопоставление 24 городов и часовых поясов задано в `geo_common.py` по идентификаторам исходного справочника. Spark переводит время из UTC по правилам IANA, учитывая сезонный перевод там, где он применяется.

## Размещение в учебном окружении

Откройте [учебное окружение](https://infra.education-services.ru/activate/dataproc-jupyter). В карточке «Jupyter Notebook · Spark и DataProc» нажмите «Создать», если окружения ещё нет. Когда окружение будет готово, откройте Jupyter из окна подключения и загрузите архив `de-project-sprint-7-ready.zip`. Откройте терминал Jupyter в папке с архивом и выполните:

```bash
unzip de-project-sprint-7-ready.zip
cd de-project-sprint-7-main

mkdir -p /lessons/dags /lessons/scripts
cp src/scripts/*.py /lessons/scripts/
cp src/dags/sprint7_geo_marts.py /lessons/dags/

curl -fL https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv -o geo.csv
hdfs dfs -mkdir -p /user/master/data/geo/sprint7/reference
hdfs dfs -copyFromLocal -f geo.csv /user/master/data/geo/sprint7/reference/geo.csv
hdfs dfs -ls /user/master/data/geo/sprint7/reference/
hdfs dfs -ls /user/master/data/geo/events/ | tail -n 10
```

Если учебный HDFS даёт права на запись только в личную папку, разместите `sprint7` там и задайте пути справочника и витрин в Airflow Variables `sprint7_cities_path` и `sprint7_output_root`. Источник по умолчанию — `/user/master/data/geo/events`, параметр при необходимости меняется через `sprint7_events_path`. DAG запускает `spark-submit --master yarn`. Для другой конфигурации Spark задайте Variable `sprint7_spark_master`.

## Пробный и итоговый запуски

1. В Airflow откройте DAG `sprint7_geo_marts` и выберите **Trigger DAG w/ config**. Подставьте в пример даты из списка HDFS. `history_start` ограничивает чтение для пробного расчёта. Для проверки `home_city` возьмите диапазон больше 27 дней.

   ```json
   {
     "as_of": "YYYY-MM-DD",
     "history_start": "YYYY-MM-DD",
     "output_root": "/user/master/data/geo/sprint7/test"
   }
   ```

2. Убедитесь, что все три задачи завершились со статусом `success`. Наличие партиций можно проверить командой:

   ```bash
   hdfs dfs -ls '/user/master/data/geo/sprint7/test/*/date=*'
   ```

   В ячейке PySpark в Jupyter проверьте схемы и строки:

   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.getOrCreate()
   spark.conf.set("spark.sql.session.timeZone", "UTC")
   root = "/user/master/data/geo/sprint7/test"
   run_date = "YYYY-MM-DD"
   for mart in ("users", "zones", "friends"):
       data = spark.read.parquet(f"{root}/{mart}/date={run_date}")
       data.printSchema()
       data.show(5, truncate=False)
   ```

3. Для итогового расчёта запустите DAG с конфигурацией `{"as_of": "YYYY-MM-DD"}`, где дата соответствует последней доступной партиции. Все три витрины будут записаны в `analytics` с полной доступной историей. Для автоматических обновлений включите DAG. Если нужен отдельный запуск из терминала, скрипт принимает те же настройки:

   ```bash
   spark-submit --master yarn /lessons/scripts/user_mart.py \
     --as-of YYYY-MM-DD --history-start YYYY-MM-DD \
     --output-root /user/master/data/geo/sprint7/test
   ```

## Сдача

Перенесите `README.md` и содержимое `src` из архива в клонированный репозиторий `de-project-sprint-7`, затем из его корня выполните:

```bash
git status
git add README.md src/dags src/scripts
git commit -m "Добавить геовитрины и DAG для Data Lake"
git push origin main
```

После обновления ветки `main` нажмите «Отправить на проверку» на странице проекта в Практикуме.
