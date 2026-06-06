# Execution Plan — единый порядок работ

Сводит [`deep-research-roadmap.md`](./deep-research-roadmap.md) (backend P0–P3) и [`frontend-roadmap.md`](./frontend-roadmap.md) (frontend F0–F4) в один упорядоченный план с зависимостями.

> Принятые решения (из обсуждения): reasoning-видимость — headline-фича; auth делаем (JWT/cookie, dev-bypass); i18n ru/en/es с переключателем; **Streamlit убираем полностью**, ops → Grafana; **Admin-панель пока не делаем**; дизайн — аналог home-экрана Claude (frontend §4.2).

---

## Текущий статус

- ✅ **Wave 0:** CORS-middleware (0.1).
- ✅ **Wave 1 / P0-задел:** `src/model_catalog.py`, `GET /v1/models`, `ResearchRequest.model` (валидация + persist в `graph_state`) (1.10).
- ✅ **Wave 1 / F0:** `web/` поднят (Vite+Vue3+TS+Tailwind+Pinia+Router); home-экран (spark + serif-приветствие, композер с селекторами модели/глубины, чипы), сайдбар collapsed↔expanded с «Недавними», создание ресёрча и страница отчёта. `npm run build` зелёный (1.6–1.9).
- ✅ **Wave 2 / F1:** SSE-эндпоинт `GET /v1/research/{id}/events` (status/trace/reasoning/report-дельты) и live-вью с панелью «Ход работы» + потоковым отчётом — поллинг заменён стримом (2.1, 2.4).
- ✅ **Reasoning-срез (2.2 плумбинг + 1.3 частично):** провайдер захватывает `reasoning_content` (stream), SSE шлёт `reasoning_delta`, в UI — сворачиваемый блок «Размышления»; добавлен per-call override модели (`kwargs["model"]`) и попутно исправлен латентный баг дублирования `model=` при заданном `repair_model`. **Активируется, когда модель отдаёт reasoning** (подтвердить у v4-pro или задать reasoner-модель).
- ✅ **Per-research модель в финализации:** выбранная в композере модель (из `graph_state["model"]`) прокинута через state графа → `_analyze` → все `generate`-вызовы анализатора (report/секции/синтез/починка); reasoner-модель → блок «Размышления» наполняется. Покрыто `tests/test_model_selection.py`.
- ⬜ Остаток Wave 2: экспорт-роут PDF/DOCX (2.3).
- ✅ **Wave 0 hardening:** SSRF-гард на webhook (`src/net_safety.py`, блок private/loopback/link-local/metadata; флаг `webhook_allow_private_targets`) (0.2); cheap `GET /v1/research/{id}/status` без тяжёлых агентов/LLM — `/summary` остаётся для on-demand (0.3). Покрыто `tests/test_net_safety.py`.
- ✅ **Wave 0 тесты (0.4):** suite зелёный (237 passed, 0 failed). Причина была НЕ в native-fallback/Python 3.14, а в рассинхроне тестов с осознанными изменениями + 1 регрессия + 3 «протухших» по времени теста. Починено: (код) low-источники снова в «Additional», возвращена инъекция «Report Notes», безопасный доступ к `analyzer.llm`, граф передаёт в `run_analysis` только поддерживаемые kwargs; (тесты) относительные timestamps в graph/health-тестах, формат кликабельных markdown-ссылок.
- ✅ **Wave 3 / F2:** отчёт рендерится как markdown (`MarkdownView`, markdown-it + @tailwindcss/typography — кликабельные ссылки, XSS-safe, serif-заголовки, бейджи `[S1]`, курсор при стриминге); **трёхпанельная раскладка** (сайдбар · thread · артефакт-панель) с вкладками **Report / Sources / Trail** (ленивая загрузка), `SourceCard` с грейдами качества; cheap бэкенд `GET /v1/research/{id}/sources` (без LLM) (3.5–3.7).
- ✅ **Вкладка Conflicts:** `GET /v1/research/{id}/conflicts` (читает `graph_state.detected_conflicts`, иначе пересчитывает из пула источников — без LLM) + панель «Противоречия» (тема, причина, спорные цитаты с `[Sn]`).
- ⬜ Остаток F2: экспорт PDF/DOCX через API (2.3), мобильная адаптация.
- ⬜ Остаток Wave 0: eval-харнесс (0.5), `user_id` миграция (0.6).
- ✅ **P0 цикл + бюджет (1.1, 1.2):** ветвление графа оживлено (`langgraph_*_max_loops` 0→1: replan/tie-break/verify-retry), но **бюджет-гард** (`finalize_budget_max_seconds=240`, `finalize_budget_max_analyze_passes=3`, дедлайн в state) не даёт раздуть стоимость/латентность; `_budget_ok` гейтит все ветвления. Suite зелёный (237). reasoning_content passthrough + per-research model — сделаны ранее (часть 1.3).
- ✅ **P0 завершён (1.4, 1.5):** детектор «нет прогресса» (`branch_stalled` — если волна replan/tie-break не дала новых уникальных источников, дальнейшее ветвление не запускается); gap-анализ (`ReplanAgent._llm_queries`) маршрутизируется на reasoning-модель при заданном `DEEPSEEK_REASONER_MODEL` (opt-in, иначе базовая модель). Suite зелёный (237).
- ✅ **P1 / F3 — план перед запуском (основной срез):** режим `plan_first` — orchestrator декомпозирует, план кладётся в `graph_state` со статусом `PLAN_REVIEW` (задачи/джобы пока не создаются); эндпоинты `GET/PUT /v1/research/{id}/plan` + `POST .../plan/approve` (создаёт задачи из правленого плана и стартует поиск). Фронт: тумблер «План» в композере (вкл по умолчанию), `PlanCard` с редактируемыми под-вопросами/запросами и «🚀 Запустить». Покрыто сервис-тестами.
- ✅ **P1 завершён — уточняющие вопросы:** `ClarifierAgent` (0–3 вопроса; для ясного запроса `[]` → сразу план) + статус `CLARIFYING` + `GET /clarifications` / `POST /clarify` (ответы аугментируют prompt декомпозиции, фоновый re-decompose → план). Фронт: `ClarifyCard` (ответить/пропустить). Поток: запрос → (уточнения) → план → live-ресёрч → отчёт → чат. Покрыто 2 сервис-тестами.
- ✅ **Чат с исследованием (grounded follow-up):** `ChatAgent` отвечает строго по собранным источникам + отчёту с цитатами `[Sn]`; эндпоинты `GET/POST /v1/research/{id}/messages`, история в `graph_state["messages"]`. Фронт: диалог в thread-колонке + закреплённый ввод (доступен после завершения ресёрча). Покрыто сервис-тестом.
- ✅ **Стриминг ответов чата:** `POST /v1/research/{id}/messages/stream` (SSE через thread+queue мост) + `streamChatAnswer` на фронте (парсер SSE поверх `fetch`, т.к. EventSource не умеет POST) — ответ печатается токен-за-токеном в плейсхолдер. ⬜ Фоллоу-ап: эскалация в мини-поиск при пробеле в источниках.
- ✅ **Полировка UX (часть):** кликабельные `[Sn]` (ведут на URL источника, маппинг из раздела «Источники» отчёта); показ стоимости/токенов (`llm_token_usage` из `/status`); **светлая/тёмная тема** (CSS-переменные RGB-каналами → работают alpha-модификаторы; переключатель в сайдбаре, persist в localStorage; `prose dark:prose-invert`). ⬜ Остаток: действия в истории (удалить/переименовать/поиск).
- ✅ **i18n (ru/en/es):** `vue-i18n` + полные локали, **переключатель языка** в сайдбаре (persist в localStorage), все видимые строки переведены (`$t`/`te`-fallback для динамических ключей статуса/шагов); скелетон загрузки истории.

---

## Карта зависимостей

```
Wave 0  Гигиена/фундамент ─────────────┐ (разблокирует всё, делается сразу)
                                        ▼
Wave 1  Backend P0 (цикл)  ∥  Frontend F0 (каркас + home-экран)
                                        ▼
Wave 2  Backend SSE        →  Frontend F1 (стрим + reasoning)      ← «момент Claude»
                                        ▼
Wave 3  Backend P2 (поиск) ∥  Frontend F2 (3-панель + артефакт)  →  выпил Streamlit
                                        ▼
Wave 4  Backend P1 (план)  →  Frontend F3 (clarify + plan card)
                                        ▼
Wave 5  Backend P3 (writer)∥  Frontend F4 (темы, i18n, auth, polish)

∥ = параллелится   → = жёсткая зависимость
```

**Критический путь до демо «выглядит как Claude и показывает рассуждение»:** Wave 0 → 1 → 2.

---

## Wave 0 — Гигиена и фундамент (сразу, параллельно)

Независимые задачи, нужные при любом сценарии. Снимают долги до публичного релиза.

| # | Задача | Где | Тип |
|---|---|---|---|
| 0.1 | **CORS-middleware** | `src/api/app.py` | S |
| 0.2 | **Закрыть SSRF** в `webhook_url` (allowlist + блок приватных диапазонов) | `research_service.py` | S |
| 0.3 | **Разделить `/summary`** на дешёвый `/status` + ленивый `/insights`; убрать LLM-replan из read-пути | `research_service.py`, `app.py` | M |
| 0.4 | **Починить тесты/native fallback** (собрать Rust-модуль или выровнять pure-Python) | `core/rust_accel.py`, CI | M |
| 0.5 | **Eval-харнесс** (золотой набор 15–25 запросов + метрики) | `scripts/eval_*.py` | M |
| 0.6 | **Ранняя миграция `user_id`** (nullable) в `researches` | `db/models.py`, alembic | S |

**Done:** тесты зелёные; `/status` дешёвый; webhook безопасен; есть бейзлайн-метрики на eval-наборе.

---

## Wave 1 — Агентное ядро (P0) ∥ Каркас фронта (F0)

### Backend P0 — оживить цикл
| # | Задача | Тип |
|---|---|---|
| 1.1 | Развыключить ветвление графа (лимиты > 0, конфигурируемо) | M |
| 1.2 | **Бюджет-менеджер** (итерации/источники/токены/время + авто-стоп) | M |
| 1.3 | Гибридная маршрутизация моделей `planner/writer/repair` **+ проброс `reasoning_content`** | M |
| 1.4 | Детектор «нет прогресса» (стоп без новых уникальных источников) | S |
| 1.5 | Gap-анализ (`ReplanAgent`) на reasoning-модели | S |

### Frontend F0 — каркас + home-экран (дизайн §4.2)
| # | Задача | Тип |
|---|---|---|
| 1.6 | Скаффолд `web/`: Vite + Vue3 + TS + Tailwind + shadcn-vue + Router + Pinia + Vue Query | M |
| 1.7 | Typed API-клиент (генерация из `/openapi.json`) | S |
| 1.8 | **Home-экран** = аналог Claude (icon-rail, spark + serif-приветствие, композер с селектором глубины, чипы-шаблоны) | M |
| 1.9 | История (`GET /v1/research`), создание (`POST`), просмотр отчёта (read-only), удаление; поллинг статуса | M |
| 1.10 | **Выбор модели**: `GET /v1/models` + `ResearchRequest.model` (per-research, валидация `model_catalog`) + селектор в композере (V4 Pro / V4 Flash) | M |

**Done:** HARD делает 2–4 итерации со стопом по бюджету; SPA создаёт ресёрч и показывает готовый отчёт; home-экран визуально совпадает с референсом.

---

## Wave 2 — Стриминг (Backend SSE → Frontend F1)

«Момент Claude»: живой прогресс + видимое рассуждение.

| # | Задача | Где | Тип |
|---|---|---|---|
| 2.1 | **SSE-эндпоинт** `GET /v1/research/{id}/events` (события: status/plan/trace/**reasoning**/report_delta/source/budget/done) | backend | M |
| 2.2 | Проброс `reasoning_content` reasoning-модели в поток | `deepseek.py`, graph | M |
| 2.3 | **Экспорт через API** `GET /v1/research/{id}/export?format=pdf\|docx` | backend | M |
| 2.4 | F1: `streamStore` на EventSource; `ProgressTrace` (narrative + сворачиваемый reasoning-блок); стрим отчёта; `SourceChips`; бюджет/ETA | frontend | L |

**Done:** отчёт «печатается» в реальном времени; сворачиваемый блок «Размышления» показывает ход мысли модели; источники появляются по ходу.

---

## Wave 3 — Глубокий поиск (P2) ∥ Полная раскладка (F2) → выпил Streamlit

### Backend P2
| # | Задача | Тип |
|---|---|---|
| 3.1 | Абстракция `SearchBackend` + Tavily/Exa рядом с DDG (фолбэк) | M |
| 3.2 | Кэш поиска/извлечений (Postgres + TTL) | M |
| 3.3 | **pgvector NotesStore**: эмбеддинги конспектов, дедуп, retrieval | L |
| 3.4 | Researcher конспектирует источник → notes (дешёвая модель) | M |

### Frontend F2
| # | Задача | Тип |
|---|---|---|
| 3.5 | Трёхпанельная раскладка + **артефакт-панель** (вкладки Report/Sources/Conflicts/Trail) | L |
| 3.6 | `MarkdownView` (markdown-it + Shiki) + кликабельные `[S1]` | M |
| 3.7 | `SourceCard` с грейдами качества; экспорт PDF/DOCX | M |
| 3.8 | Заглушки `ClarifyCard`/`PlanCard` (под Wave 4) | S |

| 3.9 | **Декомиссия Streamlit**: убрать сервис `ui` из compose; ops остаётся в Grafana | S |

**Done:** медиана 40–100 уникальных источников на HARD без бана; продуктовый Claude-вид; Streamlit удалён.

---

## Wave 4 — Планирование (P1 → F3)

| # | Задача | Где | Тип |
|---|---|---|---|
| 4.1 | `ClarifierAgent` (0–3 уточнения, пропускаемо) | backend | M |
| 4.2 | `PlannerAgent` (sub-questions) + статусы `CLARIFYING/PLANNING/AWAITING_APPROVAL` | backend + alembic | M |
| 4.3 | Эндпоинты `clarify` / `plan` (get/put) / `plan/approve` | backend | M |
| 4.4 | F3: `ClarifyCard` + редактируемый `PlanCard` + «Approve & Run»; наполнить `ProgressTrace` нарративом | frontend | L |

**Done:** пользователь правит план до запуска; уточнения поднимают релевантность на eval-наборе.

---

## Wave 5 — Writer/Verifier (P3) ∥ Polish (F4)

### Backend P3
| # | Задача | Тип |
|---|---|---|
| 5.1 | Writer `outline → секции → сшивка` для всех глубин | L |
| 5.2 | Per-claim confidence + блок «что не удалось подтвердить» | M |
| 5.3 | Финальный critic-проход (покрытие плана vs отчёт) | M |

### Frontend F4
| # | Задача | Тип |
|---|---|---|
| 5.4 | Темы light/dark; **переключатель языка** (ru/en/es, порт словарей из Streamlit) | M |
| 5.5 | **Auth** (JWT/httpOnly-cookie, email+password, dev-bypass `AUTH_DISABLED`) + scoping по `user_id` | L |
| 5.6 | Responsive, пустые/ошибочные/loading-состояния, a11y, горячие клавиши | M |

**Done:** продукт готов к публичному релизу (auth, i18n, темы, мобайл).

---

## Поперечные треки (идут всё время)

- **Eval** (с Wave 0): прогонять после каждой backend-волны, сравнивать с бейзлайном и публичным Gemini Deep Research.
- **Бюджет/стоимость**: телеметрия токенов/стоимости в каждом отчёте (база уже есть).
- **Документация**: держать роадмапы как источник правды при изменениях.

---

## Остаточные решения (дефолты, если не возразите)

- UI-кит — **shadcn-vue**; хостинг — **static-SPA на nginx**; транспорт — **SSE**; вёрстка — **desktop-first** (мобайл в F4).
- Модель: базовая **`deepseek-v4-pro`** (уже переключено в `.env`/`config.py`); reasoner для planner/gap — вариант с `reasoning_content` (**проверить, есть ли он у v4-pro**, иначе задать `DEEPSEEK_REASONER_MODEL`).
- Платный поиск (Tavily/Exa, Wave 3) — нужен бюджет; если нет — P2 работает на DDG с меньшей глубиной.
