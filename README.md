# Flink Academy

A local study tool for Apache Flink (and Apache Beam on Flink). It has 16 levels that unlock one by one, animated step-by-step diagrams in every lesson, quizzes, a Pipeline Lab that simulates a Flink cluster and draws the job graph like the real dashboard, lab challenges, a config explorer, a TaskManager memory calculator, a clickable Web UI tour and a references page.

## Start

* **Windows:** double-click `start.bat`. It starts a small Python server and opens http://127.0.0.1:8765
* **Any OS:** `python main.py`
* **No Python?** Open `web/index.html` directly. Everything works, but progress is saved only in that browser.

The server needs Python 3.6 or newer and only the standard library (no pip installs). Progress is saved to `data/progress.json`, and resetting from the app keeps a `.bak.json` backup.

## Settings

Edit `.env` (copy it from `.env.example`) to change the port or turn off auto-opening the browser.

## Optional real cluster

`real-cluster/docker-compose.yml` starts Flink 1.20 at http://localhost:8081. Exercises are in the app under **Real cluster lab**.

## Structure

```
main.py                 entry point
config/settings.py      .env -> typed settings
models/progress.py      progress document
services/               HTTP server, JSON persistence, browser launcher
utils/env_loader.py     stdlib .env parser
web/                    the single-page app (no build step, no internet needed)
  js/content/l01..l16   lessons + quizzes
  js/sim/               Flink job simulator (chaining, ship strategies, slots, backpressure)
  js/widgets/           interactive lesson widgets
  js/diagrams/          animated SVG lesson diagrams (play / step / replay)
real-cluster/           docker-compose for a real Flink cluster
```
