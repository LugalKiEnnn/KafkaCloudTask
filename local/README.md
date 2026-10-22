# Локальный кластер

## Запуск

```bash
./generate-certs.sh        # пересоздаёт secrets/ целиком
docker compose up -d
```
Остановить с удалением данных: `docker compose down -v`. После `down -v` нужно заново зарегистрировать схему.

## Контейнеры

| Контейнер | Что делает | Нормальное состояние |
|---|---|---|
| `zookeeper` | Метаданные кластера: брокеры, топики, ACL, SCRAM-пользователи | Up (healthy) |
| `kafka-users` | Один раз создаёт SCRAM-пользователей в ZooKeeper | **Exited (0)** |
| `kafka-1..3` | Брокеры | Up (healthy) |
| `kafka-init` | Один раз создаёт топики и ACL, печатает `--describe` | **Exited (0)** |
| `schema-registry` | REST-сервис схем на `localhost:8081`, хранит схемы в топике `schemas` | Up |
| `kafka-ui` | Веб-интерфейс на `localhost:8080` | Up |

## Сертификаты (generate-certs.sh)
Кроме сертификатов скрипт создаёт в `secrets/config/`:
- `zookeeper.sasl.jaas.conf` — пользователь ZooKeeper;
- `kafka_server_jaas.conf` — с каким логином брокер ходит к другим брокерам (SCRAM, `admin`) и в ZooKeeper (DIGEST, `zkadmin`);
- `admin.properties` — настройки подключения для CLI-команд внутри контейнеров.

## Защита

- **Аутентификация клиентов:** SASL/SCRAM-SHA-512 на всех listener'ах, в том числе между брокерами.
- **Взаимный TLS:** брокер требует сертификат клиента и проверяет его по CA, клиент проверяет сертификат брокера и имя хоста (`ssl.endpoint.identification.algorithm=https`).
- **ZooKeeper:** подключение только с SASL (`requireClientAuthScheme=sasl`), брокеры ставят ACL на свои znodes (`zookeeper.set.acl=true`). Клиент без SASL получает `Insufficient permission`.
- **Авторизация:** `AclAuthorizer`, `allow.everyone.if.no.acl.found=false`, суперпользователь только `admin`.

Права, которые выдаёт `kafka-init` (`init-topics-acl.sh`):

| Пользователь | Права |
|---|---|
| `producer` | `--producer` на `practice-topic` |
| `consumer` | `--consumer` на `practice-topic` и любую группу |
| `schema-registry` | All на топик `schemas` и на группы |
| `nifi` | `--consumer` на `practice-topic` и группу `nifi-group` |
| `admin` | суперпользователь |

