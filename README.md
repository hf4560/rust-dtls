# rust-dtls

Черновой Rust-порт пары программ `client/server` из Go-примера.

## Что уже есть

- Бинарник `client`:
  - парсит аргументы (`--vk-link` или `--yandex-link`, `--peer`, `--listen` и т.д.);
  - запрашивает TURN-креды для VK и Yandex Telemost;
  - поднимает базовый UDP relay (локальный сокет <-> `--peer`).
- Бинарник `server`:
  - поднимает базовый UDP relay (`--listen` <-> `--connect`).

## Ограничения текущего этапа

- DTLS-обфускация пока **не реализована** (в `client` и `server` выводится предупреждение).
- Полноценный TURN Allocate/Auth pipeline пока **не реализован** (в `client` креды получаются, но трафик пока идет напрямую в `--peer`).

## Запуск

```bash
cargo run --bin server -- --listen 0.0.0.0:56000 --connect 1.2.3.4:9001
```

```bash
cargo run --bin client -- \
  --listen 127.0.0.1:9000 \
  --peer 1.2.3.4:56000 \
  --vk-link "https://vk.com/call/join/XXXX"
```
