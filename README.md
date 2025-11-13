# Run

Locally (gradle needed)

```sh
gradle build
gradle run
```

In container (docker needed)

```sh
docker compose up --build
docker compose down
```

Connecting to kafka cli

```sh
docker exec -it kafka-app bash
./gradlew run
```

---

# Serwis 1: KafkaManager

- ✅ Tworzy localnie serwer kafki używjaac EmbeddedKafka (normalnie używa się tego do testów integracyjncyh, ale my
  chcemy żeby to był stale działający serwer)

# Serwis 2: KafkaProducer:

- ✅ Będzie tworzył wiadomości w kafce (załóżmy 100 wiadomości, po 10s kolejne 100 wiadomości, po kolejnych 10s 20
  wiadomości z nową wartością dla starego klucza)
- ✅ Każda wiadomosć musi mieć unikatowy klucz

# Serwis 3: Kafka Consumer:

- ✅ Przed konsumpcją będzie wymagał unikalnego Id sesji (consumerID)
- ✅ Wiadomości kafki będą zapisywane do bazy postgreSQL (wystarczy Klucz- [ ] Wartość. nawet w formie 1 - [ ] "A",
  2 - [ ] "B" itd)
- ✅ Czyli, aplikacja startuje i pyta w command line: podaj Id sesji (consumerID) i drugi opcjonalny parametr: Timestamp
- ✅ Jeśli aplikacja zostanie nagle zatrzymana, po restarcie i otrzymaniu tego samego Id sesji, ma kontynuować
  konsumpcję od ostatnio otrzymanej wiadomości
- ✅ Dla nowego Id sesji (będzie to też consumerID dla kafki) ma rozpocząć konsumpcję od pierwszej
  wiadomości
- ✅ Zadna wiadomość nie może zostać utracona
- ✅ Aplikacja powinna mieć możliwość rekonsumpcji: po podaniu parametru Id sesji or Timestamp wiadomości, nastąpi
  re-konsumpcja OD wiadomości z podanym timestamp

---

Warto przeczytać: czym jest Key compaction w kafce

Na co warto zwrócić uwagę: gdy kafka consumer padnie, ale nie będzie to graceful shutdown, ponowne połączenie nie będzie
konsumowało wiadomości od razu: kafka balancer pierw będzie myślał że stary consumer wciąż jest aktywny, dopóki nie
minie timeout. Będzie to widać w logach kafki. Greaceful shutdown consumera kafki to wywyłanie conusmer.close()

Należy zwrócić uwagę na parametr kafki o nazwie autocommit: on sprawia żę wiadomość odczytana nie będzie przesyłana
ponownie, dlatego warto comitować wiadomości ręcznie po zapisaniu do bazy, tak żeby uniknąć straty wiadomości, ale może
da się to rozwiązać inaczej.
