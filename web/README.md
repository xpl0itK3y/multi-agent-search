# Deep Research — Web (Vue SPA)

Claude-подобный фронтенд для deep-research бэкенда. Фаза **F0** (см. `../docs/frontend-roadmap.md`):
home-экран, сайдбар с историей (Recents), создание ресёрча, просмотр отчёта.

## Стек
Vue 3 + TypeScript + Vite + Pinia + Vue Router + Tailwind CSS.

## Запуск (dev)

```bash
cd web
npm install
npm run dev          # http://localhost:5173
```

Бэкенд должен быть поднят на `http://localhost:8000` (Vite проксирует `/v1` и `/health` на него,
поэтому CORS в dev не нужен). Если бэкенд на другом адресе — задай `VITE_API_PROXY`.

```bash
# в корне репозитория
uvicorn src.api.app:app --reload --port 8000
```

## Сборка

```bash
npm run build        # type-check (vue-tsc) + vite build -> dist/
npm run preview
```

## Что уже есть (F0)
- Home-экран: spark + serif-приветствие, композер с селектором **модели** (`GET /v1/models`)
  и **глубины**, чипы-шаблоны.
- Сайдбар: collapsed ↔ expanded, «Недавние» из `GET /v1/research`, user-card.
- Создание ресёрча (`POST /v1/research`, c выбранной моделью) и страница отчёта с поллингом статуса.

## Дальше (по roadmap)
- **F1:** SSE-стрим (живой отчёт + блок «Размышления»).
- **F2:** трёхпанельная раскладка + артефакт-панель (Report/Sources/Conflicts/Trail), markdown+Shiki.
- **F3:** карточка плана + уточнения. **F4:** темы, i18n, auth.
