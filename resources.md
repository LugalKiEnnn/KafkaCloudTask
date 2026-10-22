# Описание использованных ресурсов в Yandex Cloud (план, кластер не создавался)

## Описание ПО
| ПО        | Версия |
|:----------|:------:|
| Kafka     |  3.7   |
| Zookeeper | управляется Yandex Cloud |

## Описание железа
### Kafka хост x3:
- Платформа Intel Ice Lake 
- 2 core CPU
- 8 Gb RAM
- 32 Gb SSD

### Zookeeper хост x3 (создаётся Yandex Cloud автоматически):
- Платформа Intel Ice Lake
- 2 core CPU
- 8 Gb RAM
- 10 Gb SSD

# Локальные ресурсы
- Ubuntu 24.04
- Intel(R) Core(TM) i7-14700KF (3.40 GHz)
- 32 Gb RAM
- 300 Gb HDD

## ПО локального кластера
| ПО              | Версия |
|:----------------|:------:|
| Confluent Platform (cp-kafka, cp-zookeeper, cp-schema-registry) | 7.7.1 |
| Kafka           | 3.7    |
| ZooKeeper       | 3.8.4  |
| Kafka UI        | 0.7.2  |
| Java (клиенты)  | 17     |
| Apache NiFi     | 1.21.0 |
| PostgreSQL      | 16     |

Локально все 3 брокера, ZooKeeper, NiFi и PostgreSQL запущены в Docker на одной машине, поэтому аппаратные параметры хостов выше относятся к облачному варианту.
