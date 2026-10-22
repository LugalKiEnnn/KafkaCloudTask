# Задание 2. Интеграция Kafka с Apache NiFi

NiFi читает сообщения из топика `practice-topic` локального кластера (задание 1) и записывает их в таблицу `messages` в PostgreSQL.

## Состав

| Файл | Что внутри |
|---|---|
| [docker-compose.yml](docker-compose.yml) | NiFi 1.21.0 и PostgreSQL 16, подключены к сети `kafka-local` кластера |
| [Dockerfile](Dockerfile) | Образ NiFi с JDBC-драйвером PostgreSQL и готовым потоком |
| [flow.json.gz](flow.json.gz) | Поток NiFi: процессоры и controller services |
| [postgres/init.sql](postgres/init.sql) | Таблица `messages` |
| [console-consumer.sh](console-consumer.sh) | Чтение топика через `kafka-avro-console-consumer` |

Продюсер и консьюмер — из задания 1 ([producer/](../producer), [consumer/](../consumer)).

## Поток NiFi

`ConsumeKafkaRecord_2_6` → `PutDatabaseRecord` → `LogAttribute`

- **Чтение из Kafka** (`ConsumeKafkaRecord_2_6`): брокеры `kafka-1,2,3:19092`, SASL_SSL, SCRAM-SHA-512, пользователь `nifi`, группа `nifi-group`, сертификат клиента `local/secrets/nifi`. Значения читаются `AvroReader` по схеме из Schema Registry и пишутся в JSON.
- **Запись в Postgres** (`PutDatabaseRecord`): INSERT в таблицу `messages`, поле `createdAt` попадает в колонку `created_at`.
- **Лог записи** (`LogAttribute`): пишет в лог NiFi атрибуты и содержимое записанных сообщений.

Пользователь `nifi` и его права (чтение `practice-topic` в группе `nifi-group`) создаются в `local/create-users.sh` и `local/init-topics-acl.sh`.

## Запуск

Кластер из задания 1 должен быть запущен, схема зарегистрирована.

```bash
cd nifi
cp .env.example .env
docker compose up -d --build
```

NiFi UI: http://localhost:8090/nifi. Поток запускается автоматически.

Проверка:
```bash
./scripts/run-producer.sh local                       # из корня проекта
docker logs nifi | grep -A 20 LogAttribute            # лог NiFi
docker exec postgres psql -U postgres -d practice -c "SELECT * FROM messages"
cd nifi && ./console-consumer.sh                      # содержимое топика
```

## Результат

- Поток в NiFi: [docs/nifi-image.png](../docs/nifi-image.png)
- Запущенные сервисы: [docs/nifi-docker-ps.png](../docs/nifi-docker-ps.png)
- Топик с данными: [docs/console-consumer.txt](../docs/console-consumer.txt), [docs/nifi-consumer.png](../docs/nifi-consumer.png)
- Лог NiFi: [docs/nifi-log.png](../docs/nifi-log.png)
- Записи в PostgreSQL: [docs/nifi-postgres.png](../docs/nifi-postgres.png)
