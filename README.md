# Спринт 7. Геоаналитика в Data Lake

На PySpark построены три витрины. DAG `sprint7_geo_marts` запускает их в Airflow ежедневно в 04:00 UTC. Общий модуль `geo_common.py` читает данные и определяет ближайший город по формуле гаверсинуса. Spark передаёт модуль в YARN через `--py-files`.

| Витрина | Что рассчитывается |
| --- | --- |
| `users` | Текущий и домашний город, последовательность и число визитов, местное время. Домашний город определяется по последнему визиту длительностью больше 27 календарных дней. Возврат считается новым визитом. |
| `zones` | Недельные и месячные количества сообщений, реакций, подписок и регистраций по городам. Регистрация определяется по первому сообщению. Для событий без координат используется последний известный город пользователя. |
| `friends` | Уникальные пары пользователей с общим каналом, на расстоянии до 1 км и без прежней переписки. |

## Данные и проверка

| Слой | HDFS и формат |
| --- | --- |
| События | `/user/master/data/geo/events/date=YYYY-MM-DD`, дневные партиции Parquet |
| Справочник | `/user/s23483c714ac049239310927bg1/sprint7/reference/geo.csv`, CSV с координатами австралийских городов |
| Тестовые витрины | `/user/s23483c714ac049239310927bg1/sprint7/test/<витрина>/date=YYYY-MM-DD`, Parquet по запросу |
| Витрины для аналитиков | `/user/s23483c714ac049239310927bg1/sprint7/analytics/<витрина>/date=YYYY-MM-DD`, дневные снимки Parquet |

Тест на данных с 12 по 21 июня 2022 года прошёл успешно. Итоговый ручной запуск `manual__2026-11-23T17:03:43+00:00` успешно рассчитал все три витрины по доступной истории до 21 июня включительно. В учебном терминале подтверждены три маркера `_SUCCESS`:

```bash
hdfs dfs -ls '/user/s23483c714ac049239310927bg1/sprint7/analytics/*/date=2022-06-21/_SUCCESS'
```

Чтобы повторить расчёт, положите файлы из `src/scripts` в `/lessons/scripts`, а `src/dags/sprint7_geo_marts.py` в `/lessons/dags`. В Airflow Variables задайте `sprint7_cities_path` и `sprint7_output_root` по таблице выше. В новом окружении используйте свою папку в HDFS. [Скачать исходный справочник городов](https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv)

```bash
hdfs dfs -mkdir -p /user/s23483c714ac049239310927bg1/sprint7/reference
hdfs dfs -copyFromLocal -f geo.csv /user/s23483c714ac049239310927bg1/sprint7/reference/geo.csv
```

В форме ручного запуска укажите:

```json
{
  "as_of": "2022-06-21",
  "output_root": "/user/s23483c714ac049239310927bg1/sprint7/analytics"
}
```

## Как отправить проект в GitHub

Скачайте архив `de-project-sprint-7-ready.zip` в «Загрузки». Выполните на Mac в обычном Терминале:

```bash
archive="$HOME/Downloads/de-project-sprint-7-ready.zip"
workdir="$(mktemp -d)"
unzip -q "$archive" -d "$workdir" &&
git clone --branch main https://github.com/IAskarov/de-project-sprint-7.git "$workdir/repo" &&
cd "$workdir/repo" &&
mkdir -p src/dags src/scripts &&
cp "$workdir/de-project-sprint-7-main/README.md" README.md &&
cp "$workdir/de-project-sprint-7-main/src/dags/sprint7_geo_marts.py" src/dags/ &&
cp "$workdir/de-project-sprint-7-main/src/scripts/"*.py src/scripts/ &&
git add README.md src/dags/sprint7_geo_marts.py src/scripts/*.py &&
git diff --cached --stat &&
git commit -m "Добавить геовитрины и DAG для Data Lake" &&
git push origin main
```

Пуш выполняется из аккаунта с правом записи в [репозиторий IAskarov/de-project-sprint-7](https://github.com/IAskarov/de-project-sprint-7). После пуша проверьте ветку `main` и нажмите «Отправить на проверку» в Практикуме.
