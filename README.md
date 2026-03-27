# rust-dtls

Rust-реализация UDP-forwarder пары `client/server` под ваш сценарий (с сохранением UDP/QUIC трафика).

## Client

- По умолчанию слушает: `127.0.0.1:10000`.
- По умолчанию форвардит в: `217.28.222.148:443`.
- Может получать TURN-креды Telemost через:
  1. `GET https://cloud-api.yandex.ru/.../connection`
  2. WebSocket HELLO (`offerAnswerMode=["SEPARATE"]`) с `Origin` и `User-Agent`.
- После получения TURN-кредов поднимает UDP forwarder в стиле вашего Python-примера:
  - local socket принимает от клиента;
  - remote socket `connect()` к target;
  - двунаправленный relay без изменения payload (QUIC-friendly).

Запуск (прямой UDP-forward, как в вашем Python варианте):

```bash
cargo run --bin client -- \
  --listen-host 127.0.0.1 --listen-port 10000 \
  --target-ip 217.28.222.148 --target-port 443
```

Запуск с Telemost шагом (опционально, не блокирует старт форвардера):

```bash
cargo run --bin client -- \
  --yandex-link "https://telemost.yandex.ru/j/00057695313831" \
  --listen-host 127.0.0.1 --listen-port 10000 \
  --target-ip 217.28.222.148 --target-port 443
```

## Server

- По умолчанию слушает: `0.0.0.0:8443`.
- По умолчанию форвардит в: `127.0.0.1:443`.
- Логика такая же, как у client-forwarder: UDP `recv_from -> send`, `recv -> send_to`.

Запуск:

```bash
cargo run --bin server --
```

Или явно:

```bash
cargo run --bin server -- --listen 0.0.0.0:8443 --connect 127.0.0.1:443
```
