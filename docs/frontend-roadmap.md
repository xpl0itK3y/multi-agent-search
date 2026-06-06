# Frontend Roadmap — Vue SPA «как у Claude»

Замена Streamlit-UI на полноценный Vue-фронтенд в эстетике веб-версии Claude.

> Статус: черновик v1. Парный документ к [`deep-research-roadmap.md`](./deep-research-roadmap.md) — фазы фронта (F1/F3) зависят от backend-фаз (P1/P3), см. §9 «Сиквенсинг».

---

## 1. Зачем уходим со Streamlit

Streamlit хорош для прототипа, но для продукта тупиковый:
- перезапускает весь скрипт на каждое действие → нет контроля над состоянием/анимацией;
- почти нет контроля над вёрсткой, темами, микро-взаимодействиями;
- плохо тянет live-стриминг (нужен «как Claude» token-by-token);
- доказательство боли — git-история: `suppress page-dimming`, `prevent history flash`, `fragment rerun scope`, `_form_prompt widget-bound key`. Это драка с фреймворком вместо построения UX.

**Ключевой факт, делающий переход дешёвым:** бэкенд уже отдаёт чистый REST API (`src/api/app.py`). Streamlit — лишь один потребитель. Vue-SPA подключается к тому же API; ядро не трогаем.

---

## 2. Принцип: разделяем user-UI и ops-UI

Текущий `streamlit_app.py` (2200 строк) — это **не только ресёрч**, там тонна операционных вьюх. Повторять их в Vue — огромный объём ради того, что видите только вы.

| Зона | Содержимое (текущие Streamlit-функции) | Куда |
|---|---|---|
| **User UI** | `_render_create_research`, `_render_sidebar_history`, `_render_research_details`, `_render_report_collapsible`, `_render_task`, `_render_source`, `_render_graph_trail` | → **Vue SPA** (новое, красивое) |
| **Ops UI** | `_render_queue_overview`, `_render_job_card/_section`, `_render_graph_step_metrics`, `_render_graph_alerts`, `_render_graph_alert_trend`, `_render_maintenance_summary`, `_render_operational_health` | → **Grafana** (метрики/health, уже в compose); операционные *действия* пока через API/`curl` |

**Решение: Streamlit убираем полностью.** Дашборды наблюдаемости уходят в Grafana (Prometheus+Loki уже подключены). **Отдельную Admin-панель пока НЕ делаем** — на этом этапе не нужна; немногие операционные действия (requeue dead-letter, recover-stale, maintenance) остаются доступны через существующие API-эндпоинты/`curl`, к UI для них вернёмся позже при необходимости. Это срезает объём в разы и фокусирует усилия целиком на пользовательском интерфейсе.

---

## 3. Стек

| Слой | Выбор | Почему |
|---|---|---|
| Фреймворк | **Vue 3 + TypeScript + `<script setup>`** | требование |
| Сборка | **Vite** | мгновенный HMR, стандарт Vue |
| Роутинг | **Vue Router** | SPA-навигация |
| Стейт | **Pinia** | официальный, простой |
| Данные/кэш | **TanStack Query (Vue Query)** | поллинг статуса, ретраи, инвалидация |
| Стили | **Tailwind CSS** | Claude-эстетика = utility-first |
| UI-компоненты | **shadcn-vue (Radix Vue)** | минимализм Claude, не «тяжёлая» либа, копируем компоненты в репо |
| Markdown | **markdown-it** + **Shiki** | Shiki = подсветка как в VS Code/Claude |
| Иконки | **lucide-vue-next** | те же иконки, что в Claude |
| Стриминг | **SSE (EventSource)** | ощущение «как Claude» вместо поллинга |
| i18n | **vue-i18n** | портируем ru/en/es из Streamlit-словарей |
| Тесты | **Vitest + Vue Test Utils** + Playwright (e2e) | |

**Почему SPA, а не Nuxt/SSR:** продукт за логином, SEO не нужен → статическая SPA проще в деплое (nginx). Если позже потребуется публичный SEO-лендинг — отдельный Nuxt-сайт, не смешиваем.

---

## 4. Раскладка «как у Claude» (карта экранов)

Claude.ai = **сайдбар истории · центральная лента-диалог · правая панель-артефакт**. Маппинг на deep research:

```
┌────────────┬─────────────────────────────────┬──────────────────────────┐
│  SIDEBAR   │        RESEARCH THREAD          │     ARTIFACT PANEL        │
│            │                                 │   (откр. по клику/готов)  │
│ + New      │  [user] исходный запрос         │  ┌─ Report ─ Sources ─    │
│            │                                 │  │  Conflicts ─ Trail ─┐  │
│ История:   │  [clarify] уточнения (P1)       │  │                     │  │
│  • Ресёрч1 │  [plan] карточка плана + Run(P1) │  │  rich markdown      │  │
│  • Ресёрч2 │                                 │  │  + кликаб. [S1]     │  │
│  • …       │  [trace] живая «трасса мысли»   │  │  + export PDF/DOCX  │  │
│            │   (сворачиваемо, Claude-стайл)  │  │                     │  │
│ ⚙ ops →    │  [sources] чипы по мере находки │  └─────────────────────┘  │
│  Streamlit │  [done] ссылка на отчёт →       │                          │
└────────────┴─────────────────────────────────┴──────────────────────────┘
```

### Дерево компонентов (эскиз)

```
App
├─ AppSidebar (collapsed icon-rail ↔ expanded)
│   ├─ Brandmark + SearchButton + CollapseToggle
│   ├─ NewResearchButton
│   ├─ PrimaryNav (Researches · Collections · Templates · Customize)
│   ├─ RecentsList → RecentResearchItem (трунк. заголовок из prompt)
│   └─ UserCard (avatar · name · plan · account-menu)
├─ ResearchThread
│   ├─ QueryComposer            (создание + depth-селектор)
│   ├─ ClarifyCard              (P1, заглушка в F2)
│   ├─ PlanCard                 (P1, заглушка в F2; editable sub-questions)
│   ├─ ProgressTrace            (P3; collapsible, streaming decisions[])
│   ├─ SourceChips
│   └─ ThreadStatusBar          (статус/бюджет/ETA)
└─ ArtifactPanel
    ├─ TabReport (MarkdownView + CitationLink)
    ├─ TabSources (SourceCard, грейды качества)
    ├─ TabConflicts
    ├─ TabGraphTrail
    └─ ExportMenu (PDF/DOCX)
```

### Pinia-сторы

- `researchStore` — текущий ресёрч: статус, summary, report, graph_trail, budget.
- `historyStore` — список ресёрчей (TanStack Query).
- `streamStore` — состояние SSE-подключения, буфер событий/дельт.
- `uiStore` — тема (light/dark), открыта ли артефакт-панель, активная вкладка, язык.

### 4.1 Видимость рассуждения — headline-фича

Пользователь должен видеть, **как система думает** — как extended thinking у Claude / reasoning у GPT. Это не опция, а ключевой элемент доверия к deep research и наш дифференциатор (Gemini это прячет). Два слоя:

1. **Reasoning модели (raw)** — `reasoning_content` от reasoning-модели (DeepSeek-R1) на шагах планирования и gap-анализа. Стримится в сворачиваемый блок «Размышления» Claude-стайл (по умолчанию свёрнут, разворачивается по клику).
2. **Narrative trace (агентный)** — высокоуровневые шаги оркестрации из `decisions[]`: «строю план… ищу по q1… нашёл 6 источников… пробел в q4, до-ищу… пишу раздел…».

Оба слоя стримятся через SSE (`reasoning_delta` + `trace_step`) и **остаются доступными после завершения** (свёрнуты), как у Claude. Компонент `ProgressTrace` рендерит обе дорожки по фазам (Planning · Researching · Gap-analysis · Writing · Verifying).

> Бэкенд-зависимость: на шагах planner/gap-analyst использовать reasoning-модель, которая отдаёт `reasoning_content` отдельно от ответа (deep-research-roadmap §7), и пробросить его в SSE.

### 4.2 Дизайн-референс: домашний экран Claude (по скриншоту)

Цель — **визуальный аналог** home-экрана Claude.ai (тёмная тема), адаптированный под deep research.

**Анатомия референса:**

| Зона референса | Что на скриншоте | Наш аналог |
|---|---|---|
| Левый **icon-rail** (~64px, тёмнее фона) | toggle сайдбара, `+`, чаты, проекты, кластер, портфель, `</>`, палитра, внизу install + аватар `D` | toggle · **`+` New research** · **История** · (Saved/Collections позже) · **Тема** (палитра) · внизу **аватар + переключатель языка** |
| Верх-право | «призрак» (temporary chat) | пропускаем на старте |
| Центр | coral **spark** (8-лучевая звезда) + **serif**-приветствие «Good afternoon, denis» | spark + «Добрый день, {имя}» (serif-дисплей) |
| **Композер** (большая скруглённая карта) | placeholder, слева `+`, справа **`Opus 4.8 Max ▾`** + mic + voice | placeholder «О чём провести исследование?», слева `+` (вложить URL/файл), справа **селектор модели** (`V4 Pro ▾` / `V4 Flash`) + **селектор глубины** (`Balanced ▾`: Quick/Balanced/Deep Dive), mic — опц. |
| **Чипы-шорткаты** | `</> Code`, `✎ Write`, `🎓 Learn`, `☕ Life stuff`, `▲ From Drive` | шаблоны ресёрча: **Market scan · Tech compare · Lit review · From URL · …** |
| Низ-лево | аватар `D` | профиль/выход |

**Дизайн-токены (dark-first, как у Claude):**

```css
--bg:        #262624;  /* тёплый charcoal, основной фон */
--bg-rail:   #1F1E1D;  /* icon-rail, чуть темнее */
--surface:   #30302E;  /* композер/карточки */
--border:    rgba(255,255,255,.08);
--text:      #ECEAE3;  /* тёплый off-white */
--text-muted:#9A9890;
--accent:    #D97757;  /* coral — spark, акценты */
--radius-card: 16px;   /* композер/карточки (rounded-2xl) */
--radius-chip: 9999px; /* чипы — full */
```

- **Шрифты:** дисплей-**serif** для приветствия/заголовков (открытая альтернатива проприетарному Copernicus — `Tiempos`/`Source Serif 4`/`Lora`); **sans** для тела (`Inter`/системный). Точную копию шрифта Claude не воспроизводим — берём близкую открытую.
- **Раскладка:** одна центральная колонка, композер `max-w-3xl` (~768px), много воздуха, тёмная тема по умолчанию (light — в F4 через CSS-переменные).
- **Состояние «пусто/домой»** = этот экран; после первого запроса он сменяется трёхпанельной раскладкой из §4.

> Это «домашний/пустой» экран. Трёхпанельный режим (сайдбар · лента · артефакт) из §4 включается, когда есть активный/выбранный ресёрч.

### 4.3 Сайдбар: collapsed ↔ expanded + история (по 2-му скриншоту)

Сайдбар имеет два состояния, переключаемые иконкой-тоглом:
- **collapsed** — узкий icon-rail (1-й скриншот);
- **expanded** — полный сайдбар (2-й скриншот) с историей.

**Анатомия expanded (сверху вниз):**

| Зона референса | На скриншоте | Наш аналог |
|---|---|---|
| **Brandmark** + search + collapse | «Claude» (serif), лупа, тогл | название продукта (serif) · поиск · тогл |
| `+ New chat` | создать чат | **`+ New research`** |
| Первичная навигация | Chats (active) · Projects · Artifacts · Customize | **Researches** (active) · Collections · Templates · Customize |
| Группа «Products» | Code · Design (🧪 beaker) | пропускаем (нерелевантно) |
| **«Recents»** + фильтр | список чатов (трунк. заголовки) | **список ресёрчей** — заголовок из `prompt` (трунк. ~32 симв.), активный подсвечен |
| **User-card** (низ) | аватар `D` · «denis / Pro plan» · download · ↕ account | аватар · имя · план · меню аккаунта (+ переключатель языка) |

**Бэкенд:** «Recents» = уже существующий `GET /v1/research?limit=` (`ResearchHistoryItem` содержит `prompt`, `status`, `created_at`) — **новых эндпоинтов не нужно**. Заголовок деривируется из `prompt` (как Claude генерит заголовки чатов). Поиск (лупа) — сначала клиентская фильтрация, опционально `?q=` позже. Статусы ресёрчей (running/completed/failed) показываем точкой/иконкой у пункта.

### 4.4 Выбор модели пользователем

Как «Opus 4.8 Max ▾» у Claude — в композере **селектор модели**. Источник правды — `src/model_catalog.py` (уже создан):

| Модель | Когда |
|---|---|
| **V4 Pro** (`deepseek-v4-pro`, default) | глубокий ресёрч; точнее, дороже/медленнее |
| **V4 Flash** (`deepseek-v4-flash`) | быстрые/лёгкие прогоны; дешевле |

Поток:
- `GET /v1/models` → отдаёт каталог (`id/label/description/tier/reasoning/default`) для рендера селектора.
- `ResearchRequest.model` (опц.) — выбранная модель сохраняется **per-research**; агенты этого ресёрча используют её как базовую.
- **Валидация на сервере** обязательна: `resolve_model_id()` отклоняет произвольные id (защита от навязанной/дорогой модели) → фолбэк на дефолт.
- Role-routing поверх выбора: reasoner/repair-роли берут свои override-модели (`deepseek_reasoner_model`/`deepseek_repair_model`) или фолбэк на выбранную базовую.
- Поле `reasoning` в каталоге управляет тем, показывать ли блок «Размышления» для модели (для Flash — скрыт). Каталог легко расширяется новыми моделями («и т.д.»).

---

## 5. Что трогаем в бэкенде (минимально)

| Изменение | Где | Тип | Зачем |
|---|---|---|---|
| **CORS-middleware** | `src/api/app.py` | S | SPA с `:5173` → API `:8000` |
| **`GET /v1/models`** + `ResearchRequest.model` (per-research, валидация через `model_catalog`) | `app.py`, `schemas.py`, `research_service.py`, `db/models.py`+alembic | M | селектор модели в композере (§4.4); каталог `src/model_catalog.py` уже есть |
| **SSE-эндпоинт** `GET /v1/research/{id}/events` | новый роут + сервис | M | live-стрим вместо поллинга — сердце «Claude-feel» |
| **Экспорт через API** `GET /v1/research/{id}/export?format=pdf\|docx` | вынести из `report_export.py` в роут | M | сейчас экспорт живёт внутри Streamlit-процесса |
| **Разделить `/summary`** на дешёвый `/status` и ленивый `/insights` | `research_service.py` | M | сейчас `get_research_summary` гоняет source_critic+evidence+claim_verifier+**LLM-replan** на каждый вызов — с поллящей SPA это больно |
| **Auth** (решено: делаем) | новый слой `src/auth/` | L | продукт публичный → нужна; JWT в httpOnly-cookie, email+password; в dev флаг `AUTH_DISABLED=true`, чтобы не блокировать F0 |
| **`user_id` в `researches`** (ранняя дешёвая миграция) | `src/db/models.py`, alembic | S | зарезервировать scoping данных по пользователю заранее, чтобы не делать болезненный backfill после auth |
| Plan/clarify эндпоинты | приходят из deep-research **P1** | — | зависимость, не дублировать |

### SSE-контракт (черновик)

`GET /v1/research/{id}/events` → `text/event-stream`, события:

```
event: status_change   data: {"status":"researching","iteration":2}
event: plan_ready       data: {"plan":[{"id":"q1","question":"...","status":"open"}]}   # P1
event: trace_step       data: {"step":"gap_analysis","narrative":"Покрыто 3/5, до-ищу q4"} # P3
event: reasoning_delta  data: {"phase":"planning","delta":"...сырые токены размышления модели..."} # §4.1
event: source_found     data: {"url":"...","domain":"...","source_quality":"high"}
event: report_delta     data: {"delta":"...накопленный partial..."}   # из streaming_callback анализатора
event: budget           data: {"spent_sources":31,"spent_tokens":142000,"max_tokens":400000}
event: done             data: {"status":"completed"}
event: error            data: {"detail":"..."}
```

У вас уже есть `streaming_callback` в `AnalyzerAgent` и сохранение partial_report → нужно лишь выставить поток наружу (in-memory pub/sub на研究_id, либо опрос `graph_state`/partial в генераторе SSE как первый дешёвый вариант).

> Деплой-нюанс: SSE требует отключения буферизации на прокси (`proxy_buffering off;` в nginx, `X-Accel-Buffering: no`).

---

## 6. Дорожная карта по фазам

### F0 — Каркас + read-only на текущем API
**Цель:** рабочая SPA, видимый выигрыш, без стриминга.
- Репозиторий `web/`: Vite + Vue3 + TS + Tailwind + shadcn-vue + Router + Pinia + Vue Query.
- API-клиент (typed) + типы из схем (можно сгенерить из OpenAPI `/openapi.json`).
- **CORS** на бэкенде.
- Экраны: сайдбар-история (`GET /v1/research`), создание (`POST /v1/research`), просмотр отчёта (`GET /v1/research/{id}/report` + `/summary`), удаление.
- Поллинг статуса через Vue Query (временно).
- **Критерий:** можно создать ресёрч и увидеть готовый отчёт с источниками; деплой собирается.

### F1 — Стриминг (момент «как Claude»)
**Цель:** живой прогресс.
- Backend: **SSE-эндпоинт** (+ проброс `reasoning_content` reasoning-модели) + экспорт-роут.
- Frontend: `streamStore` на EventSource, `ProgressTrace` (narrative + сворачиваемый reasoning-блок), стрим `report_delta`/`reasoning_delta`, `SourceChips` по `source_found`, бюджет/ETA в статус-баре.
- Убрать поллинг там, где есть SSE.
- **Критерий:** отчёт «печатается» в реальном времени, источники появляются по ходу.

### F2 — Полная Claude-раскладка + артефакт-панель
**Цель:** продуктовый вид.
- Трёхпанельная раскладка, артефакт-панель с вкладками **Report / Sources / Conflicts / Graph trail**.
- `MarkdownView` (markdown-it + Shiki), кликабельные `[S1]` → подсветка источника.
- `SourceCard` с грейдами качества (`source_quality`, `source_type`, `confidence` — данные уже есть в summary).
- Экспорт PDF/DOCX через новый API-роут.
- **Заглушки** `ClarifyCard`/`PlanCard` (под P1) — чтобы не переделывать раскладку дважды.
- **Критерий:** визуальный паритет с Claude по чистоте; полный отчёт читается комфортно.

### F3 — План и уточнения (зависит от backend P1)
**Цель:** ключевой UX-сдвиг к Gemini/Claude.
- `ClarifyCard`: 0–3 уточнения до старта (пропускаемо).
- `PlanCard`: редактируемые sub-questions + «Approve & Run».
- `ProgressTrace` наполняется нарративом `decisions[]` (backend P3).
- **Критерий:** пользователь правит план до запуска; трасса читается как «ход мысли».

### F4 — Polish
- Темы light/dark (Claude-палитра: тёплые нейтрали, мягкие бордеры, `rounded-lg`).
- Responsive/мобайл, пустые/ошибочные/loading-состояния, скелетоны.
- Горячие клавиши, i18n (порт ru/en/es из Streamlit-словарей).
- Доступность (a11y), фокус-менеджмент.
- Auth (если публичный).

---

## 7. Деплой

- **Dev:** `vite dev` (:5173) с прокси `/v1` и `/health` → `api:8000`.
- **Prod:** multi-stage Dockerfile (`node build` → `nginx` отдаёт статику); nginx проксирует `/v1`/`/health`/`/v1/.../events` на API с `proxy_buffering off` для SSE.
- **docker-compose:** заменить сервис `ui` (streamlit) на `web` (nginx) **или** оставить оба на разных портах на переходный период (Streamlit как ops-панель).

---

## 8. Дизайн-система (Claude-ориентир)

- **Типографика:** один чистый sans (Inter/системный), щедрый line-height, ограниченная ширина контента (~`max-w-3xl`) для читабельности отчёта.
- **Цвет:** нейтральная тёплая база, минимум акцентов, акцент на контенте, не на хроме.
- **Поверхности:** мягкие бордеры (`border`), скругления (`rounded-lg`), едва заметные тени, много воздуха.
- **Движение:** короткие easing-переходы; стриминг-каретка/«печатается»; раскрытие артефакт-панели как у Claude.
- **Тёмная тема** с первого дня (через CSS-переменные/Tailwind `dark:`).

---

## 9. Сиквенсинг с deep-research-roadmap

```
Backend:  P0 ──▶ P1 (план/уточнения) ──▶ P3 (трасса/writer)
Frontend: F0 ─▶ F1 (SSE) ─▶ F2 (раскладка) ─▶ F3 (план/уточнения)  ─▶ F4
                                    └── F3 требует backend P1 ──────┘
```
**Рекомендация:** F0–F2 строить на текущем API сразу (быстрый выигрыш), `ClarifyCard`/`PlanCard` заложить как заглушки в F2, наполнить в F3 когда подъедет backend P1. Так не переписываем раскладку дважды.

---

## 10. Риски и митигации

| Риск | Митигация |
|---|---|
| Дорогой `/summary` под поллящей SPA | Разделить на `/status`+`/insights`, убрать LLM из read-пути (см. §5 и deep-research roadmap) |
| SSE рвётся через прокси/буферизацию | `proxy_buffering off`, `X-Accel-Buffering: no`, heartbeat-комментарии, авто-reconnect EventSource |
| Переделка раскладки дважды | Заглушки план/трасса в F2, выравнивание с backend P1/P3 |
| Нет auth → нельзя в публичный веб | Auth в F4 до публичного релиза (или держать за VPN/basic-auth) |
| Объём (полный SPA) | Split user/ops (§2) — ops остаётся на Streamlit/Grafana |
| Порт i18n (ru/en/es) | Словари уже есть в Streamlit — механический перенос в vue-i18n |
| Рассинхрон типов фронт/бэк | Генерация TS-типов из `/openapi.json` |

---

## 11. Оценка усилий (грубо)

| Фаза | Объём |
|---|---|
| F0 каркас + read-only | ~3–5 дней |
| F1 SSE + стриминг | ~3–5 дней (вкл. backend SSE) |
| F2 раскладка + артефакт | ~5–8 дней |
| F3 план/уточнения | ~3–5 дней (после backend P1) |
| F4 polish/auth/i18n | ~5–10 дней |

MVP (F0–F2) ≈ **2–3 недели** до «не стыдно показать».

---

## 12. Решения

**Принято:**
- **Auth** — делаем (продукт публичный). JWT в httpOnly-cookie, email+password; в dev `AUTH_DISABLED=true`. `user_id` в `researches` резервируем ранней миграцией.
- **i18n** — оставляем ru/en/es + **переключатель языка в интерфейсе** (vue-i18n, состояние в `uiStore`).
- **Streamlit** — убираем полностью. Наблюдаемость → Grafana, операционные действия → тонкая Admin-секция Vue.
- **Видимость рассуждения** — headline-фича (§4.1): raw reasoning + narrative trace, стрим по SSE.

**Осталось решить:**
1. **Хостинг фронта:** статическая SPA (nginx) — рекомендую; SSR/Nuxt только если нужен публичный SEO-лендинг.
2. **UI-кит:** подтверждаем **shadcn-vue** или предпочитаете PrimeVue / Naive UI?
3. **Мобайл:** адаптив с первого дня или desktop-first?
4. **SSE vs WebSocket:** SSE (рекомендую — односторонний поток, проще) или нужна двусторонняя интерактивность (тогда WS)?
