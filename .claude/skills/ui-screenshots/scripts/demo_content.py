# -*- coding: utf-8 -*-
"""Invented demo content for demo_server.py: realistic-looking researches for UI screenshots.
The quotes attributed to real sites are made up; nothing here is real research output."""

# ─────────────────────────────────────────────────────────────────────────────
# 1. Completed Russian research: Na-ion vs LFP for home energy storage
# ─────────────────────────────────────────────────────────────────────────────

RU_PROMPT = (
    "Натрий-ионные аккумуляторы против LFP для домашних систем хранения энергии: "
    "стоимость, ресурс, безопасность и перспективы до 2030 года"
)
RU_TITLE = "Na-ion против LFP для домашних накопителей"

RU_SOURCES = [
    {
        "source_id": "S1",
        "url": "https://www.catl.com/en/news/6401.html",
        "title": "CATL unveils Naxtra, its second-generation sodium-ion battery",
        "domain": "catl.com",
        "source_quality": "high",
        "source_type": "primary",
        "confidence": "high",
        "content": (
            "CATL today unveiled Naxtra, its second-generation sodium-ion battery. The cell reaches an "
            "energy density of 175 Wh/kg and is rated for up to 10 000 cycles. Naxtra keeps working at "
            "−40 °C and retains 90% of its capacity at −20 °C, which makes it suited to cold climates "
            "and unheated installations. CATL, BYD and HiNa are expanding sodium-ion capacity, with most "
            "output going to electric vehicles and grid storage. Натрий-ионная батарея Naxtra сохраняет "
            "работоспособность до −40 °C и заявлена на ресурс до 10 000 циклов."
        ),
    },
    {
        "source_id": "S2",
        "url": "https://www.iea.org/reports/batteries-and-secure-energy-transitions",
        "title": "Batteries and Secure Energy Transitions — IEA",
        "domain": "iea.org",
        "source_quality": "high",
        "source_type": "primary",
        "confidence": "high",
        "content": (
            "Sodium-ion chemistry could be 20–30% cheaper in materials cost than lithium iron phosphate "
            "because it avoids lithium, cobalt and copper. LFP cells today reach 160–205 Wh/kg and are "
            "rated for 6 000–8 000 cycles in stationary storage. The IEA expects cost parity between "
            "sodium-ion and LFP in 2028–2030 once manufacturing exceeds 100 GWh. Натрий-ионная химия может "
            "быть на 20–30% дешевле по стоимости материалов; паритет цен ожидается в 2028–2030 годах."
        ),
    },
    {
        "source_id": "S3",
        "url": "https://about.bnef.com/blog/lithium-ion-battery-pack-prices-see-largest-drop-since-2017/",
        "title": "Lithium-Ion Battery Pack Prices See Largest Drop Since 2017 — BloombergNEF",
        "domain": "about.bnef.com",
        "source_quality": "high",
        "source_type": "editorial",
        "confidence": "high",
        "content": (
            "The average battery pack price fell 20% in 2024 to $115 per kWh, the largest annual drop "
            "since 2017. Stationary storage systems built on LFP became cheaper faster than any other "
            "segment, and average LFP cell prices in China fell below $60 per kWh. If lithium stays "
            "cheap, LFP is likely to remain the price leader through 2030. Средняя цена LFP-ячеек в "
            "Китае опустилась ниже $60 за кВт·ч; цена пакета снизилась на 20% до $115 за кВт·ч."
        ),
    },
    {
        "source_id": "S4",
        "url": "https://www.nature.com/articles/s41560-024-01521-3",
        "title": "Safety and degradation of sodium-ion cells under deep discharge — Nature Energy",
        "domain": "nature.com",
        "source_quality": "high",
        "source_type": "primary",
        "confidence": "high",
        "content": (
            "Sodium-ion cells can be discharged to 0 V for storage and transport without damage, "
            "reducing fire risk during logistics. The onset temperature of thermal runaway is higher "
            "than for NMC and comparable to LFP. In our tests LFP cells delivered 4 000–6 000 cycles to "
            "80% capacity and retained only 60–70% of capacity at −20 °C. Натрий-ионные ячейки можно "
            "разряжать до 0 В для хранения и транспортировки; температура начала теплового разгона "
            "сопоставима с LFP."
        ),
    },
    {
        "source_id": "S5",
        "url": "https://habr.com/ru/articles/847215/",
        "title": "Год с натрий-ионным накопителем на даче: опыт и замеры",
        "domain": "habr.com",
        "source_quality": "medium",
        "source_type": "community",
        "confidence": "medium",
        "content": (
            "Собрал домашний накопитель на натрий-ионных ячейках и год гонял его в неотапливаемом доме. "
            "Независимые испытания энтузиастов фиксируют 3 000–4 000 циклов до 80% остаточной ёмкости в "
            "домашнем режиме — заметно меньше заявленных производителем 10 000 циклов. Зато при −25 °C "
            "ячейки отдают почти всю ёмкость, а LFP в том же сарае пришлось бы подогревать. "
            "Низкотемпературное поведение Na-ion — главный практический аргумент для дачных домов."
        ),
    },
    {
        "source_id": "S6",
        "url": "https://www.rbc.ru/technology_and_media/12/03/2026/65f0c1a29a79471f8c2b4d1e",
        "title": "Рынок домашних систем хранения энергии в России: цены и спрос",
        "domain": "rbc.ru",
        "source_quality": "medium",
        "source_type": "editorial",
        "confidence": "medium",
        "content": (
            "Типичная домашняя система на LFP ёмкостью 10 кВт·ч стоит 450–600 тыс. рублей с установкой. "
            "Готовые домашние системы на натрий-ионных ячейках в российской рознице практически "
            "отсутствуют, а сами ячейки Na-ion продаются по 70–90 долларов за кВт·ч — на 30–50% дороже "
            "LFP. Требования сертификации UN 38.3 и IEC 62619 для обеих химий пока одинаковы."
        ),
    },
    {
        "source_id": "S7",
        "url": "https://www.ixbt.com/news/2026/02/18/hina-sodium-ion-cells-review.html",
        "title": "Обзор натрий-ионных ячеек HiNa: плотность, цена и морозостойкость",
        "domain": "ixbt.com",
        "source_quality": "medium",
        "source_type": "editorial",
        "confidence": "medium",
        "content": (
            "Ячейки HiNa показывают удельную энергию 140–160 Вт·ч/кг и уверенно работают на морозе. "
            "Розничные ячейки Na-ion пока продаются на 30–50% дороже LFP, однако цены на Na-ion падают "
            "быстрее прогнозов. CATL, BYD и HiNa наращивают мощности, но большая часть продукции идёт в "
            "электротранспорт и сетевые накопители."
        ),
    },
    {
        "source_id": "S8",
        "url": "https://vc.ru/future/1348822-natriy-ionnye-akkumulyatory-dogonyayut-litiy",
        "title": "Натрий-ионные аккумуляторы догоняют литий: что изменилось за год",
        "domain": "vc.ru",
        "source_quality": "low",
        "source_type": "community",
        "confidence": "low",
        "content": (
            "Ячейки HiNa показывают удельную энергию 140–160 Вт·ч/кг и уверенно работают на морозе. "
            "Розничные ячейки Na-ion пока продаются на 30–50% дороже LFP, однако цены на Na-ion падают "
            "быстрее прогнозов. CATL, BYD и HiNa наращивают мощности, но большая часть продукции идёт в "
            "электротранспорт и сетевые накопители. Автор ожидает паритет цен уже в 2027 году."
        ),
    },
]

RU_TASKS = [
    {
        "description": "Текущая стоимость ячеек и систем Na-ion и LFP ($/кВт·ч) и прогнозы до 2030 года",
        "queries": ["стоимость натрий-ионных аккумуляторов 2026 $/кВт·ч", "LFP battery pack price 2024 BNEF survey"],
        "sources": ["S2", "S3", "S6"],
    },
    {
        "description": "Ресурс циклов, деградация и работа при низких температурах",
        "queries": ["sodium-ion cycle life cold temperature test", "натрий-ионный аккумулятор ресурс циклов мороз"],
        "sources": ["S1", "S5"],
    },
    {
        "description": "Безопасность: тепловой разгон, транспортировка, сертификация",
        "queries": ["sodium-ion thermal runaway 0V transport safety", "UN 38.3 IEC 62619 sodium-ion"],
        "sources": ["S4"],
    },
    {
        "description": "Производственные мощности и ключевые игроки (CATL, BYD, HiNa)",
        "queries": ["CATL Naxtra sodium-ion production capacity", "HiNa натрий-ионные ячейки обзор"],
        "sources": ["S7", "S8"],
    },
]
RU_REPLAN_TASK = {
    "description": "Реальные внедрения Na-ion в домашних накопителях и отзывы владельцев",
    "queries": ["натрий-ионный домашний накопитель отзывы", "sodium-ion home battery owner experience"],
    "sources": [],
}

RU_REPORT = """# Натрий-ионные аккумуляторы против LFP для домашних накопителей энергии

## Краткий ответ

К 2026 году LFP (литий-железо-фосфат) остаётся более зрелым и дешёвым выбором для домашних систем хранения энергии: средняя цена LFP-ячеек в Китае опустилась ниже $60 за кВт·ч [S3], а экосистема инверторов и BMS полностью отлажена [S2]. Натрий-ионные (Na-ion) ячейки выигрывают там, где важны работа на морозе и безопасность: они сохраняют работоспособность до −40 °C [S1] и допускают хранение и перевозку при нулевом заряде [S4]. Ценовое преимущество Na-ion в 20–30% по материалам пока остаётся прогнозом, а не рыночной реальностью [S2][S3].

## Сравнение ключевых параметров

| Параметр | Na-ion (2025–2026) | LFP (2025–2026) |
|---|---|---|
| Удельная энергия ячейки | 140–175 Вт·ч/кг [S1][S7] | 160–205 Вт·ч/кг [S2] |
| Заявленный ресурс | до 10 000 циклов [S1] | 6 000–8 000 циклов [S2] |
| Ресурс в независимых тестах | 3 000–4 000 циклов до 80% [S5] | 4 000–6 000 циклов до 80% [S4] |
| Ёмкость при −20 °C | ~90% [S1] | 60–70% [S4] |
| Цена ячейки, $/кВт·ч | 70–90 [S6] | 50–60 [S3] |

## Стоимость и прогнозы до 2030 года

По данным BloombergNEF, средняя цена батарейного пакета в 2024 году снизилась на 20% до $115 за кВт·ч, а стационарные системы на LFP подешевели быстрее всех сегментов [S3]. IEA оценивает, что натрий-ионная химия может быть на 20–30% дешевле по стоимости материалов благодаря отказу от лития, кобальта и меди [S2]. Однако при текущих низких ценах на карбонат лития это преимущество не реализуется: розничные ячейки Na-ion продаются на 30–50% дороже LFP [S6][S7].

- **Базовый сценарий:** паритет цен Na-ion и LFP наступает в 2028–2030 годах при масштабе производства свыше 100 ГВт·ч [S2].
- **Консервативный сценарий:** LFP сохраняет ценовое лидерство до 2030 года, если литий останется дешёвым [S3].
- **Оптимистичный сценарий:** паритет уже в 2027 году [S8] — прогноз из блога без подтверждения первичными данными.

## Ресурс и работа на морозе

CATL заявляет для второго поколения Naxtra ресурс до 10 000 циклов и сохранение 90% ёмкости при −20 °C [S1]. Независимые испытания энтузиастов фиксируют более скромные 3 000–4 000 циклов до 80% остаточной ёмкости в домашнем режиме [S5]. Для неотапливаемых помещений и дачных домов низкотемпературное поведение Na-ion — главный практический аргумент [S5][S7].

## Безопасность и сертификация

1. Na-ion ячейки можно разряжать до 0 В для хранения и транспортировки, что снижает риск возгорания при логистике [S4].
2. Температура начала теплового разгона у Na-ion выше, чем у NMC, и сопоставима с LFP [S4].
3. Требования сертификации (UN 38.3, IEC 62619) для обеих химий пока одинаковы [S6].

## Рынок и доступность

Крупнейшие производители — CATL, BYD и HiNa — наращивают мощности, но большая часть продукции идёт в электротранспорт и сетевые накопители [S1][S7]. В России готовые домашние системы на Na-ion практически отсутствуют в рознице; типичная LFP-система на 10 кВт·ч стоит 450–600 тыс. рублей с установкой [S6].

## Противоречия в источниках

- **Ресурс циклов.** Производитель заявляет до 10 000 циклов [S1], тогда как независимые тесты показывают 3 000–4 000 [S5].
- **Сроки ценового паритета.** IEA ожидает паритет к 2028–2030 годам [S2], BNEF допускает сохранение лидерства LFP до 2030 года [S3].

## Рекомендации

- Для отапливаемого дома в 2026 году выбирайте LFP: дешевле, доступнее, проверенная экосистема [S3][S6].
- Для неотапливаемых помещений и регионов с морозами ниже −20 °C рассмотрите Na-ion, если доступна гарантия производителя [S1][S5].
- Пересматривайте решение ежегодно: цены на Na-ion падают быстрее прогнозов [S7][S8].
"""

RU_CONFLICTS = [
    {
        "topic": "Ресурс циклов Na-ion в домашнем режиме",
        "reason": "Производитель указывает лабораторный ресурс, независимые тесты — реальную эксплуатацию с глубокими циклами.",
        "source_ids": ["S1", "S5"],
        "sentences": [
            "Натрий-ионная батарея Naxtra заявлена на ресурс до 10 000 циклов.",
            "Независимые испытания фиксируют 3 000–4 000 циклов до 80% остаточной ёмкости в домашнем режиме.",
        ],
    },
    {
        "topic": "Сроки ценового паритета Na-ion и LFP",
        "reason": "Прогнозы расходятся из-за разных допущений о цене карбоната лития.",
        "source_ids": ["S2", "S3", "S8"],
        "sentences": [
            "Паритет цен между Na-ion и LFP ожидается в 2028–2030 годах.",
            "Если литий останется дешёвым, LFP сохранит ценовое лидерство до 2030 года.",
            "Автор ожидает паритет цен уже в 2027 году.",
        ],
    },
]

RU_RED_TEAM = {
    "findings": [
        {
            "claim": "Na-ion будет на 20–30% дешевле LFP к 2030 году",
            "verdict": "contested",
            "challenge": "Цены LFP упали на 20% только за 2024 год; при дешёвом литии преимущество по материалам может не дойти до потребителя.",
            "source_urls": ["https://about.bnef.com/blog/lithium-ion-battery-pack-prices-see-largest-drop-since-2017/"],
        },
        {
            "claim": "Na-ion сохраняет ёмкость на морозе лучше LFP",
            "verdict": "holds",
            "challenge": "Подтверждается и производителем, и независимыми замерами при −20…−25 °C.",
            "source_urls": ["https://habr.com/ru/articles/847215/", "https://www.nature.com/articles/s41560-024-01521-3"],
        },
        {
            "claim": "Na-ion безопаснее LFP при транспортировке",
            "verdict": "qualified",
            "challenge": "Разряд до 0 В подтверждён лабораторно, но нормативные требования UN 38.3 для обеих химий пока одинаковы.",
            "source_urls": ["https://www.nature.com/articles/s41560-024-01521-3"],
        },
        {
            "claim": "Готовые домашние Na-ion системы доступны в российской рознице",
            "verdict": "refuted",
            "challenge": "Розничных предложений не найдено; встречаются только единичные импортные поставки ячеек.",
            "source_urls": ["https://www.rbc.ru/technology_and_media/12/03/2026/65f0c1a29a79471f8c2b4d1e"],
        },
    ],
    "challenged": 3,
    "held": 1,
}

RU_STANCE = {
    "applicable": True,
    "proposition": "Натрий-ионные аккумуляторы — лучший выбор для домашнего накопителя к 2030 году",
    "supports": 3,
    "opposes": 2,
    "neutral": 3,
    "dominant_side": "supports",
    "skew": 0.6,
    "sources": [
        {"source_id": "S1", "stance": "supports"},
        {"source_id": "S2", "stance": "neutral"},
        {"source_id": "S3", "stance": "opposes"},
        {"source_id": "S4", "stance": "neutral"},
        {"source_id": "S5", "stance": "supports"},
        {"source_id": "S6", "stance": "opposes"},
        {"source_id": "S7", "stance": "neutral"},
        {"source_id": "S8", "stance": "supports"},
    ],
}

RU_CROSS_LANGUAGE = {
    "query_language": "ru",
    "languages": [{"lang": "ru", "count": 4}, {"lang": "en", "count": 4}],
    "target_languages": ["en", "zh"],
    "foreign_source_count": 4,
    "monolingual": False,
    "unique_findings": [
        {"lang": "en", "finding": "BNEF: средняя цена LFP-ячеек в Китае опустилась ниже $60 за кВт·ч в 2024 году."},
        {"lang": "en", "finding": "Nature Energy: Na-ion можно хранить и перевозить при 0 В без деградации."},
    ],
}

RU_USAGE = {
    "prompt_tokens": 184320,
    "completion_tokens": 22410,
    "cache_hit_tokens": 96256,
    "total_tokens": 206730,
    "estimated_cost_usd": 0.0873,
}

RU_CHAT = [
    ("user", "А что выгоднее для дачи в Подмосковье, где зимой бывает до −25 °C, а дом не отапливается?"),
    (
        "assistant",
        "Для неотапливаемого дома с морозами до −25 °C практичнее **Na-ion**:\n\n"
        "- при −20…−25 °C натрий-ионные ячейки отдают почти всю ёмкость [S5], тогда как LFP теряет 30–40% [S4];\n"
        "- LFP в таких условиях потребует подогрева батарейного шкафа, а это дополнительный расход энергии;\n"
        "- минус — цена: ячейки Na-ion пока на 30–50% дороже LFP [S6], и готовых систем в рознице почти нет.\n\n"
        "Если дом зимой отапливается хотя бы до +5 °C, LFP остаётся более выгодным вариантом [S3].",
    ),
]

# ─────────────────────────────────────────────────────────────────────────────
# 2. Completed English research: four-day work week
# ─────────────────────────────────────────────────────────────────────────────

EN_PROMPT = (
    "Do four-day work week pilots maintain productivity? Compare the evidence from the UK, "
    "Iceland, Microsoft Japan and Valencia trials"
)
EN_TITLE = "Four-day week: does productivity hold?"

EN_SOURCES = [
    {
        "source_id": "S1",
        "url": "https://autonomy.work/portfolio/uk4dwpilotresults/",
        "title": "The results are in: the UK's four-day week pilot",
        "domain": "autonomy.work",
        "source_quality": "high",
        "source_type": "primary",
        "confidence": "high",
        "content": (
            "61 companies and around 2,900 staff took part in the UK four-day week pilot in 2022. "
            "Revenue stayed broadly flat, rising 1.4% on average over the trial. 56 of the 61 companies "
            "continued with the four-day week after the pilot. Sick days fell by about two thirds, and "
            "companies reported fewer and shorter meetings with default 30-minute slots."
        ),
    },
    {
        "source_id": "S2",
        "url": "https://www.bbc.com/news/business-57724779",
        "title": "Four-day week 'an overwhelming success' in Iceland",
        "domain": "bbc.com",
        "source_quality": "high",
        "source_type": "editorial",
        "confidence": "high",
        "content": (
            "Trials of a shorter working week in Iceland between 2015 and 2019 involving about 2,500 "
            "public sector workers were an overwhelming success. Productivity was maintained or improved "
            "in the majority of workplaces. 86% of Iceland's workforce have since moved to shorter hours "
            "or gained the right to do so."
        ),
    },
    {
        "source_id": "S3",
        "url": "https://www.theguardian.com/technology/2019/nov/04/microsoft-japan-four-day-work-week-productivity",
        "title": "Microsoft Japan tested a four-day work week and productivity jumped 40%",
        "domain": "theguardian.com",
        "source_quality": "high",
        "source_type": "editorial",
        "confidence": "medium",
        "content": (
            "Microsoft Japan gave its 2,300 employees five Fridays off in August 2019 as part of its "
            "Work-Life Choice Challenge. Sales per employee rose 39.9% compared with the same month a year "
            "earlier. The one-month experiment was too short to show long-run effects."
        ),
    },
    {
        "source_id": "S4",
        "url": "https://hbr.org/2023/02/what-a-four-day-work-week-can-teach-us-about-productivity",
        "title": "What a Four-Day Work Week Can Teach Us About Productivity",
        "domain": "hbr.org",
        "source_quality": "high",
        "source_type": "editorial",
        "confidence": "high",
        "content": (
            "Organisations that succeeded with a four-day week redesigned work rather than compressing it: "
            "fewer and shorter meetings, explicit prioritisation of deep-work blocks and clear metrics. "
            "Firms should run a pilot of at least six months with measured output metrics and define a "
            "rollback plan in advance."
        ),
    },
    {
        "source_id": "S5",
        "url": "https://elpais.com/economia/2023-06-12/valencia-semana-cuatro-dias-resultados.html",
        "title": "Valencia cierra su prueba de la semana de cuatro días",
        "domain": "elpais.com",
        "source_quality": "medium",
        "source_type": "editorial",
        "confidence": "medium",
        "content": (
            "La ciudad de Valencia probó cuatro fines de semana largos en 2023. El estudio registró menos "
            "emisiones y mayor bienestar, pero no midió la productividad. El Ayuntamiento no ha continuado "
            "el experimento."
        ),
    },
    {
        "source_id": "S6",
        "url": "https://www.nber.org/papers/w31802",
        "title": "Four-Day Workweeks: Selection, Measurement and External Validity (NBER)",
        "domain": "nber.org",
        "source_quality": "high",
        "source_type": "primary",
        "confidence": "high",
        "content": (
            "Most four-day week trials suffer from selection bias: firms that volunteer are already likely "
            "to succeed. Productivity is often self-reported rather than measured. Evidence is thinner for "
            "manufacturing, healthcare and public services."
        ),
    },
    {
        "source_id": "S7",
        "url": "https://www.ft.com/content/4a8c8d63-1f6b-4f3f-9a2d-6e1c0d5a9b21",
        "title": "Iceland's four-day week: the fine print",
        "domain": "ft.com",
        "source_quality": "medium",
        "source_type": "editorial",
        "confidence": "medium",
        "content": (
            "Critics note that most Icelandic workplaces cut hours only to 35–36 per week, not to a "
            "four-day week. Productivity in the trials was largely self-reported by managers."
        ),
    },
]

EN_TASKS = [
    {
        "description": "Outcomes of the UK 2022 four-day week pilot",
        "queries": ["UK four-day week pilot results revenue", "4 Day Week Global UK pilot 2022 retention"],
        "sources": ["S1"],
    },
    {
        "description": "Iceland 2015–2019 public-sector trials and their follow-up",
        "queries": ["Iceland four day week trial 2015 2019 results", "Iceland shorter working week criticism"],
        "sources": ["S2", "S7"],
    },
    {
        "description": "Private-sector experiments in Japan and Spain",
        "queries": ["Microsoft Japan four-day week productivity", "Valencia semana cuatro días resultados"],
        "sources": ["S3", "S5"],
    },
    {
        "description": "Methodological critiques and implementation advice",
        "queries": ["four-day week selection bias study", "how to run four-day week pilot HBR"],
        "sources": ["S4", "S6"],
    },
]

EN_REPORT = """# Do four-day work week pilots maintain productivity?

## Bottom line

Across the largest published pilots, a four-day week with no loss of pay has generally maintained output while improving wellbeing and retention [S1][S2]. The evidence is strongest for knowledge-work organisations that self-selected into trials; it is thinner for manufacturing, healthcare and public services [S4][S6].

## What the major trials found

| Trial | Participants | Productivity outcome | After the trial |
|---|---|---|---|
| UK pilot (2022) | 61 companies, ~2,900 staff | Revenue broadly flat (+1.4% on average) [S1] | 56 of 61 continued [S1] |
| Iceland (2015–2019) | ~2,500 public workers | Maintained or improved [S2] | 86% of workforce gained shorter hours [S2] |
| Microsoft Japan (2019) | ~2,300 staff | Sales per employee +39.9% [S3] | One-month experiment only [S3] |
| Valencia (2023) | City-wide, 4 long weekends | Not measured; lower emissions [S5] | Not continued [S5] |

## Why productivity holds up

- Fewer and shorter meetings, with default 30-minute slots [S1][S4].
- Explicit prioritisation of deep-work blocks and clear output metrics [S4].
- Lower absenteeism: sick days fell by about two thirds in the UK pilot [S1].

## Where the evidence is weak

1. Selection bias: firms that volunteer are already likely to succeed [S6].
2. Productivity is often self-reported rather than measured [S6][S7].
3. Short trial windows cannot show long-run effects [S3].

## Conflicting evidence

- **Size of the effect.** Microsoft Japan reported a 39.9% jump in sales per employee [S3], while the UK pilot found revenue essentially unchanged [S1].
- **What "four days" means.** Iceland's trials are often cited as an overwhelming success [S2], but most workplaces cut hours only to 35–36 per week rather than to four days [S7].

## Recommendation

Run a pilot of at least six months with measured output metrics rather than surveys, and define a rollback plan in advance [S4][S6].
"""

EN_CONFLICTS = [
    {
        "topic": "Size of the productivity effect",
        "reason": "A one-month private-sector experiment versus a six-month multi-company pilot measuring revenue.",
        "source_ids": ["S3", "S1"],
        "sentences": [
            "Sales per employee rose 39.9% compared with the same month a year earlier.",
            "Revenue stayed broadly flat, rising 1.4% on average over the trial.",
        ],
    },
]

EN_RED_TEAM = {
    "findings": [
        {
            "claim": "Four-day week pilots maintain productivity",
            "verdict": "qualified",
            "challenge": "Holds for self-selected knowledge-work firms; productivity was mostly self-reported.",
            "source_urls": ["https://www.nber.org/papers/w31802"],
        },
        {
            "claim": "Microsoft Japan's productivity rose 40%",
            "verdict": "contested",
            "challenge": "Sales per employee in a single month is a weak proxy and August is seasonally atypical.",
            "source_urls": ["https://www.theguardian.com/technology/2019/nov/04/microsoft-japan-four-day-work-week-productivity"],
        },
        {
            "claim": "Most UK pilot companies kept the four-day week",
            "verdict": "holds",
            "challenge": "56 of 61 companies continued after the pilot.",
            "source_urls": ["https://autonomy.work/portfolio/uk4dwpilotresults/"],
        },
    ],
    "challenged": 2,
    "held": 1,
}

EN_COMPARISON = {
    "options": ["UK pilot (2022)", "Iceland (2015–2019)", "Microsoft Japan (2019)"],
    "rows": [
        {
            "criterion": "Scale",
            "cells": [
                {"option": "UK pilot (2022)", "value": "61 companies, ~2,900 staff", "source_ids": ["S1"]},
                {"option": "Iceland (2015–2019)", "value": "~2,500 public workers", "source_ids": ["S2"]},
                {"option": "Microsoft Japan (2019)", "value": "~2,300 staff", "source_ids": ["S3"]},
            ],
        },
        {
            "criterion": "Duration",
            "cells": [
                {"option": "UK pilot (2022)", "value": "6 months", "source_ids": ["S1"]},
                {"option": "Iceland (2015–2019)", "value": "4 years", "source_ids": ["S2"]},
                {"option": "Microsoft Japan (2019)", "value": "1 month", "source_ids": ["S3"]},
            ],
        },
        {
            "criterion": "Productivity",
            "cells": [
                {"option": "UK pilot (2022)", "value": "Revenue +1.4%", "source_ids": ["S1"]},
                {"option": "Iceland (2015–2019)", "value": "Maintained or improved", "source_ids": ["S2"]},
                {"option": "Microsoft Japan (2019)", "value": "Sales/employee +39.9%", "source_ids": ["S3"]},
            ],
        },
    ],
    "recommendation": "The UK pilot is the most transferable evidence for a mid-size knowledge-work company.",
}

EN_USAGE = {
    "prompt_tokens": 96512,
    "completion_tokens": 14208,
    "cache_hit_tokens": 40960,
    "total_tokens": 110720,
    "estimated_cost_usd": 0.0412,
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. Running Russian research: SMRs (analyzing, with a replan loop)
# ─────────────────────────────────────────────────────────────────────────────

RUN_PROMPT = (
    "Перспективы малых модульных реакторов (SMR) в Европе и Центральной Азии до 2035 года: "
    "ключевые проекты, стоимость электроэнергии и регуляторные барьеры"
)

RUN_TASKS = [
    {
        "description": "Ключевые проекты SMR в Европе: статус и сроки",
        "queries": ["SMR projects Europe 2026 status Rolls-Royce NuScale", "малые модульные реакторы Европа проекты"],
        "sources": [
            ("world-nuclear-news.org", "Rolls-Royce SMR selected for UK fleet"),
            ("euronews.com", "Europe's small modular reactor race"),
            ("iaea.org", "SMR Book 2026 edition"),
        ],
        "status": "completed",
    },
    {
        "description": "Проекты SMR в Казахстане и Узбекистане",
        "queries": ["Казахстан АЭС малая мощность Росатом РИТМ-200", "Uzbekistan SMR Rosatom 2026"],
        "sources": [
            ("kazinform.kz", "Казахстан рассматривает малые модульные реакторы"),
            ("atomic-energy.ru", "РИТМ-200Н для Узбекистана: график строительства"),
        ],
        "status": "completed",
    },
    {
        "description": "Стоимость электроэнергии (LCOE) SMR против крупных АЭС и ВИЭ",
        "queries": ["SMR LCOE estimate 2026 $/MWh", "NuScale Carbon Free Power Project cost cancelled"],
        "sources": [
            ("ieefa.org", "Small modular reactors: still too expensive"),
            ("oecd-nea.org", "The NEA Small Modular Reactor Dashboard"),
        ],
        "status": "completed",
    },
    {
        "description": "Регуляторные барьеры и лицензирование",
        "queries": ["SMR licensing Europe harmonisation 2026", "лицензирование малых реакторов Ростехнадзор"],
        "sources": [
            ("ensreg.eu", "European SMR pre-partnership: licensing"),
            ("nrc.gov", "NuScale US460 standard design approval"),
        ],
        "status": "completed",
    },
]
RUN_REPLAN_TASKS = [
    {
        "description": "Финансирование и государственная поддержка SMR в ЕС",
        "queries": ["EU SMR industrial alliance funding 2026", "SMR state aid Poland Czech Romania"],
        "sources": [("ec.europa.eu", "European Industrial Alliance on SMRs")],
        "status": "completed",
    },
    {
        "description": "Водные ресурсы и площадки для SMR в Центральной Азии",
        "queries": ["Central Asia SMR water cooling sites", "Балхаш АЭС площадка вода"],
        "sources": [("tengrinews.kz", "Площадка у озера Балхаш: экологические риски")],
        "status": "running",
    },
]

RUN_PARTIAL_REPORT = """# Малые модульные реакторы в Европе и Центральной Азии до 2035 года

## Краткий ответ

До 2035 года в Европе реально ожидать ввода лишь первых единичных SMR: наиболее продвинуты британский проект Rolls-Royce SMR и румынский NuScale в Дойчешти [S1][S3]. В Центральной Азии ключевой игрок — Росатом с реакторами РИТМ-200Н для Узбекистана [S5]. Стоимость электроэнергии первых блоков оценивается в 90–120 $/МВт·ч, что выше, чем у крупных АЭС и ВИЭ с накопителями [S6][S7].

## Ключевые проекты

| Проект | Страна | Технология | Ожидаемый ввод |
|---|---|---|---|
| Rolls-Royce SMR | Великобритания | PWR, 470 МВт | 2033–2035 [S1] |
| Doicești | Румыния | NuScale VOYGR-6 | 2032–2033 [S3] |
| Джизакская АЭС малой мощности | Узбекистан | РИТМ-200Н, 2×55 МВт | 2031–2033 [S5] |

## Стоимость электроэнергии

По оценкам IEEFA, отменённый проект NuScale Carbon Free Power Project вышел бы на 89 $/МВт·ч даже с учётом субсидий [S6]. NEA
"""

RUN_REASONING = (
    "Сопоставляю оценки LCOE: IEEFA приводит 89 $/МВт·ч для CFPP с субсидиями, NEA даёт диапазон "
    "для first-of-a-kind блоков. Нужно явно развести FOAK и NOAK — иначе сравнение с крупными АЭС "
    "некорректно. По Казахстану данных о стоимости нет, поэтому раздел будет качественным; помечу это "
    "как пробел в покрытии…"
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Short sidebar-history items
# ─────────────────────────────────────────────────────────────────────────────

HISTORY = [
    {
        "prompt": "Лучшие практики миграции PostgreSQL 14 → 16 без простоя для продакшн-кластера на 2 ТБ",
        "title": "Миграция PostgreSQL 14 → 16 без простоя",
        "language": "ru",
        "depth": "medium",
        "status": "completed",
        "days_ago": 2,
        "report": (
            "# Миграция PostgreSQL 14 → 16 без простоя\n\n"
            "## Краткий ответ\n\nДля кластера на 2 ТБ оптимальна логическая репликация с переключением "
            "через pgBouncer: окно недоступности сокращается до секунд [S1][S2].\n\n"
            "## Шаги\n\n1. Поднять реплику PostgreSQL 16 и настроить публикацию [S1].\n"
            "2. Дождаться синхронизации и сверить счётчики строк [S2].\n"
            "3. Переключить трафик через pgBouncer PAUSE/RESUME [S3].\n"
        ),
        "sources": [
            ("postgresql.org", "https://www.postgresql.org/docs/16/logical-replication.html", "PostgreSQL 16: Logical Replication", "high"),
            ("habr.com", "https://habr.com/ru/companies/avito/articles/790512/", "Как мы обновили PostgreSQL без даунтайма", "medium"),
            ("pgbouncer.org", "https://www.pgbouncer.org/usage.html", "PgBouncer usage: PAUSE and RESUME", "high"),
        ],
    },
    {
        "prompt": "Анализ рынка электровелосипедов в Казахстане в 2025–2026 годах: объём, игроки, регулирование",
        "title": None,
        "language": "ru",
        "depth": "hard",
        "status": "failed",
        "days_ago": 3,
    },
    {
        "prompt": "Compare Tavily, Exa and Brave Search APIs for a production RAG pipeline: quality, latency, pricing",
        "title": "Search APIs for RAG: Tavily vs Exa vs Brave",
        "language": "en",
        "depth": "medium",
        "status": "completed",
        "days_ago": 6,
        "report": (
            "# Search APIs for RAG: Tavily vs Exa vs Brave\n\n"
            "## Bottom line\n\nTavily returns page content in one call and suits agent pipelines [S1]; "
            "Exa is strongest for semantic 'find similar' queries [S2]; Brave is the cheapest for high "
            "volumes of plain web search [S3].\n"
        ),
        "sources": [
            ("docs.tavily.com", "https://docs.tavily.com/documentation/api-reference/endpoint/search", "Tavily Search API reference", "high"),
            ("docs.exa.ai", "https://docs.exa.ai/reference/search", "Exa search reference", "high"),
            ("brave.com", "https://brave.com/search/api/", "Brave Search API pricing", "medium"),
        ],
    },
    {
        "prompt": "Как устроена программа льготной ипотеки «Отбасы банк» в Казахстане и кто может в неё попасть",
        "title": None,
        "language": "ru",
        "depth": "easy",
        "status": "cancelled",
        "days_ago": 12,
    },
]
