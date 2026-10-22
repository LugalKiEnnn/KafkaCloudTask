# Развёртывание и настройка Kafka-кластера в Yandex Cloud

## Структура

| Путь                                           | Что внутри                                                                                                                                     |
|------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| [resources.md](resources.md)                   | Аппаратные ресурсы и версии ПО                                                                                                                 |
| [local/](local/)                               | Локальный кластер: docker-compose, генерация сертификатов, пользователи, топики и ACL. Подробное описание в [local/README.md](local/README.md) |
| [yandex-cloud/](yandex-cloud/)                 | Тот же кластер в Managed Service for Apache Kafka: скрипт `yc`, truststore с сертификатом Yandex, Schema Registry                              |
| [schema/message.avsc](schema/message.avsc)     | Avro-схема сообщения                                                                                                                           |
| [scripts/](scripts/)                           | Общие для обоих окружений: регистрация схемы, `--describe`, запуск продюсера и консьюмера                                                      |
| [producer/](producer/), [consumer/](consumer/) | Код продюсера и консьюмера (Java 17, Avro, Schema Registry)                                                                                    |
| `local/output/`                                | Результаты проверки: вывод `--describe`, curl к Schema Registry, логи продюсера и консьюмера                                                   |
| [docs/](docs/)                                 | Скриншоты и документы                                                                                                                          |
| [nifi/](nifi/)                                 | Задание 2: интеграция с Apache NiFi, описание в [nifi/README.md](nifi/README.md) |

Все скрипты из `scripts/` принимают первым аргументом окружение: `local` или `yandex-cloud`.

## 1. Создание кластера
Вместо Yandex Cloud в связи с известными событиями был развёрнут локальный кластер по образу и подобию KafkaSecured.
Для Yandex Cloud подготовлены скрипты с теми же параметрами: если кластер в облаке станет доступен, достаточно запускать скрипты с окружением `yandex-cloud` вместо `local`.

Описание использованных настроек кластера содержится в [resources.md](resources.md).

### Локальный кластер (выполнено)

Конфигурация в [local/docker-compose.yml](local/docker-compose.yml), образы Confluent Platform 7.7.1 (Kafka 3.7, ZooKeeper 3.8). Защита такая же, как в SecuredKafka, сертификаты генерируются по тем же конфигам:
- все подключения к брокерам и между брокерами идут по SASL_SSL, механизм SCRAM-SHA-512 (как в Yandex Cloud);
- взаимный TLS: брокер проверяет сертификат клиента, клиент проверяет сертификат и имя хоста брокера;
- брокеры входят в ZooKeeper по SASL, данные в ZooKeeper закрыты ACL;
- авторизация через ACL, без явного разрешения доступа нет.

```bash
cd local
cp .env.example .env
./generate-certs.sh           # CA, keystore/truststore брокеров и клиентов, JAAS-файлы
docker compose up -d
docker compose ps
```

Контейнеры `kafka-users` (создаёт SCRAM-пользователей) и `kafka-init` (создаёт топики и ACL) отрабатывают один раз и завершаются с кодом 0, остальные должны быть в состоянии `healthy`. Kafka UI доступен на http://localhost:8080.

### Yandex Cloud

Кластер в облаке не создавался, скрипты ниже не проверены.

Скрипт [yandex-cloud/create-cluster.sh](yandex-cloud/create-cluster.sh) создаёт кластер, топики и пользователей через `yc` CLI:
- 3 зоны доступности (`ru-central1-a`, `-b`, `-d`) по 1 брокеру, класс хоста `s3-c2-m8` (Intel Ice Lake, 2 vCPU, 8 ГБ), диск `network-ssd` 32 ГБ, публичный доступ;
- топики и пользователи с теми же параметрами, что и в локальном кластере.

```bash
cd yandex-cloud
cp .env.example .env           # заполнить пароли
yc init
./create-cluster.sh           # параметры кластера сохраняются в output/cluster.txt
./create-truststore.sh        # сертификат Yandex CA в truststore для клиентов
docker compose up -d          # Schema Registry на localhost:8081
```

## 2. Настройка кластера

### Параметры кластера (значения по умолчанию для всех топиков)

| Параметр | Значение | Зачем |
|---|---|---|
| `log.cleanup.policy` | `delete` | Старые сегменты удаляются по времени. Для потока событий нужна история за период, а не последнее значение по ключу (`compact`) |
| `log.retention.ms` | `604800000` (7 дней) | Хватает, чтобы перечитать данные после сбоя консьюмера или обработать их повторно |
| `log.segment.bytes` | `1073741824` (1 ГБ) | Удаляются только закрытые сегменты целиком. 1 ГБ — значение по умолчанию: при учебной нагрузке сегмент не переполняется, и данные хранятся до конца retention |
| `default.replication.factor` | `3` | Каждая партиция есть на всех трёх брокерах |
| `min.insync.replicas` | `2` | Запись с `acks=all` подтверждается, когда сообщение есть минимум на 2 брокерах |
| `auto.create.topics.enable` | `false` | Топики создаются только явно |
| `offsets.topic.replication.factor`, `transaction.state.log.replication.factor` | `3` | Служебные топики тоже реплицированы |

Локально они заданы переменными `KAFKA_LOG_*` и др. в [local/docker-compose.yml](local/docker-compose.yml), в Yandex Cloud — флагами `--log-retention-ms` и `--log-segment-bytes` в [create-cluster.sh](yandex-cloud/create-cluster.sh).

### Топик `practice-topic`

3 партиции, фактор репликации 3. Настройки топика повторяют настройки кластера, но заданы явно (без префикса `log.`), чтобы топик не зависел от умолчаний кластера: `cleanup.policy=delete`, `retention.ms=604800000`, `segment.bytes=1073741824`, `min.insync.replicas=2`.

Локально топик создаёт [local/init-topics-acl.sh](local/init-topics-acl.sh), в облаке — [create-cluster.sh](yandex-cloud/create-cluster.sh).

Вывод `kafka-topics.sh --describe` сохраняется в [local/output/topic-describe.txt](local/output/topic-describe.txt):

```bash
./scripts/describe-topic.sh local
```
```
Topic: practice-topic	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,cleanup.policy=delete,segment.bytes=1073741824,retention.ms=604800000
	Topic: practice-topic	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: practice-topic	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: practice-topic	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```
Лидеры партиций распределены по разным брокерам, все реплики в ISR.

Скриншот: [docs/describe-topic.png](docs/describe-topic.png).

### Пользователи и права

| Пользователь | Права |
|---|---|
| `admin` | Суперпользователь (роль admin в Yandex Cloud) |
| `producer` | Запись в `practice-topic` |
| `consumer` | Чтение `practice-topic` в любой группе |
| `schema-registry` | Чтение и запись топика `schemas` |
| `nifi` | Чтение `practice-topic` в группе `nifi-group` (задание 2) |

## 3. Настройка Schema Registry

Confluent Schema Registry 7.7.1 в Docker на `localhost:8081`. Схемы хранятся в топике `schemas` (1 партиция, фактор репликации 3, `cleanup.policy=compact`: для каждой схемы нужна только последняя запись). Режим совместимости по умолчанию `BACKWARD`: новая версия схемы должна читать данные, записанные предыдущей.
- Локально: сервис `schema-registry` в [local/docker-compose.yml](local/docker-compose.yml), подключается к брокерам по SASL_SSL с сертификатом клиента.
- Yandex Cloud: [yandex-cloud/docker-compose.yml](yandex-cloud/docker-compose.yml), подключается к облачным брокерам по SASL_SSL с сертификатом Yandex CA.

Схема сообщения [schema/message.avsc](schema/message.avsc): поля `id`, `number`, `text`, `createdAt`. Регистрируется в subject `practice-topic-value` (стратегия TopicNameStrategy):

```bash
./scripts/register-schema.sh local
```

Результат сохраняется в [local/output/schema-registry.txt](local/output/schema-registry.txt):
```
$ curl http://localhost:8081/subjects
["practice-topic-value"]
$ curl -X GET http://localhost:8081/subjects/practice-topic-value/versions
[1]
```

Скриншот: [docs/scheme.png](docs/scheme.png).

## 4. Проверка работоспособности

Продюсер и консьюмер на Java 17, сериализация Avro через Schema Registry, класс сообщения генерируется из `schema/message.avsc`. Подключение: SASL_SSL, SCRAM-SHA-512, truststore, а для локального кластера ещё и keystore клиента. Пути к хранилищам и пароли скрипты передают через переменные окружения из `.env` выбранного окружения.

- Продюсер ([producer/](producer/)) отправляет 10 сообщений с `acks=all` и идемпотентностью. Схему он сам не регистрирует (`auto.register.schemas=false`), а берёт последнюю версию из Schema Registry.
- Консьюмер ([consumer/](consumer/)) читает топик в группе `practice-consumer-group` и печатает каждое сообщение.

```bash
./scripts/run-producer.sh local     # лог: local/output/producer.log
./scripts/run-consumer.sh local     # лог: local/output/consumer.log, остановка Ctrl+C
```

Продюсер:
```
INFO kafka.KafkaMessageProducer - Сообщение доставлено: key = e72c1c7e-..., number = 1, topic = practice-topic, partition = 1, offset = 1
INFO kafka.KafkaMessageProducer - Сообщение доставлено: key = 04a2dba3-..., number = 2, topic = practice-topic, partition = 1, offset = 2
```
Консьюмер:
```
INFO kafka.MessageConsumer - Получено сообщение: key = e72c1c7e-..., number = 1, text = Тестовое сообщение №1, partition = 1, offset = 1
INFO kafka.MessageConsumer - Получено сообщение: key = 04a2dba3-..., number = 2, text = Тестовое сообщение №2, partition = 1, offset = 2
```
Отправлено и получено 10 из 10 сообщений.

Скриншоты: [docs/producer.png](docs/producer.png), [docs/consumer.png](docs/consumer.png).

Для облака те же команды выполняются с аргументом `yandex-cloud`.
