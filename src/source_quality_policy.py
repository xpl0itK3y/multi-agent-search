from dataclasses import dataclass


@dataclass(frozen=True)
class TopicPolicy:
    query_tokens: tuple[str, ...]
    premium_domains: frozenset[str]
    secondary_domains: frozenset[str]
    weak_domains: frozenset[str]
    weak_domain_substrings: tuple[str, ...]
    strong_editorial_tokens: tuple[str, ...]
    weak_signal_tokens: tuple[str, ...]
    generic_listicle_tokens: tuple[str, ...]


CONSUMER_TECH_POLICY = TopicPolicy(
    query_tokens=(
        "smartphone",
        "smartphones",
        "phone",
        "phones",
        "смартфон",
        "смартфоны",
        "смартфонов",
        "телефон",
        "телефоны",
        "телефонов",
        "айфон",
        "флагман",
        "флагманы",
        "android",
        "iphone",
        "flagship",
        "camera phone",
        "mobile",
        "galaxy",
        "pixel",
        "oneplus",
        "xiaomi",
        "oppo",
        "honor",
        "foldable",
        "chipset",
        "benchmark",
    ),
    premium_domains=frozenset(
        {
            "gsmarena.com",
            "dxomark.com",
            "pcmag.com",
            "cnet.com",
            "techradar.com",
            "techadvisor.com",
            "notebookcheck.net",
            "androidauthority.com",
            "tomsguide.com",
            "theverge.com",
            "apple.com",
            "news.samsung.com",
            "samsung.com",
            "blog.google",
            "store.google.com",
            "google.com",
            "oneplus.com",
            "mi.com",
            "xiaomi.com",
            "oppo.com",
            "honor.com",
            "phonearena.com",
            "androidcentral.com",
            "androidpolice.com",
            "9to5google.com",
            "9to5mac.com",
            "ign.com",
        }
    ),
    secondary_domains=frozenset(
        {
            "gizmochina.com",
            "gadgets360.com",
            "stuff.tv",
            "independent.co.uk",
        }
    ),
    weak_domains=frozenset(
        {
            "gizbot.com",
            "timesnownews.com",
            "vertu.com",
            "axis-intelligence.com",
            "gadgetph.com",
            "asumetech.com",
            "techspecs.info",
            "techindeep.com",
            "technicalforum.org",
            "macprices.net",
            "nyongesasande.com",
            "brandvm.com",
            "mobileradar.com",
            "techtimes.com",
            "dialoguepakistan.com",
            "techarc.net",
            "wirefly.com",
            "news.wirefly.com",
            "techrankup.com",
            "asymco.com",
            "futureinsights.com",
            "rank1one.com",
            "gistoftheday.com",
            "cashkr.com",
            "theconsumers.guide",
            "techoble.com",
            "rave-tech.com",
            "couponscurry.com",
            "giftpals.com",
            "accio.com",
            "igeniusphonerepair.com",
            "mensjournal.com",
            "haofinder.com",
            "aigadgetech.com",
            "technomemo.com",
            "mobilemall.co",
        }
    ),
    weak_domain_substrings=(
        "buyersguide",
        "buyers-guide",
        "rankings-guide",
        "top-phones",
        "best-phones",
        "best-smartphones",
        "smartphone-rankings",
        "consumers.guide",
        "futureinsights",
        "rank1one",
        "gistoftheday",
    ),
    strong_editorial_tokens=(
        "tested",
        "review",
        "reviews",
        "benchmark",
        "benchmarks",
        "camera test",
        "hands-on",
        "official",
        "launch",
        "vs",
        "comparison",
        "lab test",
        "editor's choice",
        "battery life",
        "camera comparison",
        "performance test",
    ),
    weak_signal_tokens=(
        "rumor",
        "rumors",
        "rumour",
        "rumoured",
        "expected to launch",
        "launch date",
        "price in",
        "upcoming",
        "most anticipated",
        "what to expect",
        "predictions for",
    ),
    generic_listicle_tokens=(
        "best phones",
        "best smartphones",
        "top phones",
        "top smartphones",
        "best phone",
        "best smartphone",
        "top 10 best",
        "for every budget",
        "buyers guide",
        "buying guide",
        "should you choose",
        "which should you choose",
        "smartphone rankings",
        "rankings revealed",
        "performance ranking",
        "best camera phones",
        "best camera phone",
        "best gaming phones",
        "phone buying guide",
        "smartphone buying guide",
        "top flagship phones",
    ),
)


DOCS_PROGRAMMING_POLICY = TopicPolicy(
    query_tokens=(
        "api",
        "apis",
        "sdk",
        "docs",
        "documentation",
        "reference",
        "manual",
        "tutorial",
        "guide",
        "framework",
        "frameworks",
        "library",
        "libraries",
        "python",
        "javascript",
        "typescript",
        "react",
        "fastapi",
        "flask",
        "django",
        "openai",
        "node.js",
        "nodejs",
        "документация",
        "справочник",
        "руководство",
        "библиотека",
        "фреймворк",
        "туториал",
        "python api",
    ),
    premium_domains=frozenset(
        {
            "docs.python.org",
            "developer.mozilla.org",
            "platform.openai.com",
            "openai.com",
            "fastapi.tiangolo.com",
            "docs.djangoproject.com",
            "flask.palletsprojects.com",
            "react.dev",
            "nodejs.org",
            "docs.github.com",
            "learn.microsoft.com",
            "docs.aws.amazon.com",
            "cloud.google.com",
            "kubernetes.io",
            "postgresql.org",
        }
    ),
    secondary_domains=frozenset(
        {
            "stackoverflow.com",
            "realpython.com",
            "css-tricks.com",
            "web.dev",
            "roadmap.sh",
            "freecodecamp.org",
        }
    ),
    weak_domains=frozenset(
        {
            "medium.com",
            "towardsdatascience.com",
            "geeksforgeeks.org",
            "w3schools.com",
            "tutorialspoint.com",
            "dev.to",
            "hashnode.dev",
            "dzone.com",
        }
    ),
    weak_domain_substrings=(
        "blogspot",
        "substack",
        "medium.com",
    ),
    strong_editorial_tokens=(
        "official documentation",
        "api reference",
        "reference guide",
        "quickstart",
        "installation",
        "migration guide",
        "tutorial",
        "docs",
        "documentation",
        "manual",
    ),
    weak_signal_tokens=(
        "top frameworks",
        "best libraries",
        "comparison blog",
        "opinion",
        "thoughts on",
    ),
    generic_listicle_tokens=(
        "best python frameworks",
        "best javascript frameworks",
        "top frameworks",
        "top libraries",
        "best libraries",
        "framework comparison",
        "which framework should you choose",
    ),
)


NEWS_CURRENT_POLICY = TopicPolicy(
    query_tokens=(
        "news",
        "latest",
        "today",
        "current",
        "update",
        "updates",
        "announced",
        "launch",
        "launched",
        "breaking",
        "press release",
        "regulation",
        "earnings",
        "новости",
        "последние",
        "сегодня",
        "обновление",
        "обновления",
        "анонс",
        "запуск",
        "релиз",
        "регулятор",
    ),
    premium_domains=frozenset(
        {
            "reuters.com",
            "apnews.com",
            "bloomberg.com",
            "ft.com",
            "wsj.com",
            "bbc.com",
            "www.bbc.com",
            "nytimes.com",
            "sec.gov",
            "europa.eu",
            "whitehouse.gov",
            "newsroom.google",
            "about.fb.com",
            "openai.com",
            "blog.google",
            "news.samsung.com",
        }
    ),
    secondary_domains=frozenset(
        {
            "theverge.com",
            "techcrunch.com",
            "cnn.com",
            "theguardian.com",
            "axios.com",
            "cnbc.com",
        }
    ),
    weak_domains=frozenset(
        {
            "futureinsights.com",
            "analyticsinsight.net",
            "trendhunter.com",
            "eventify.io",
            "startus-insights.com",
            "giftpals.com",
        }
    ),
    weak_domain_substrings=(
        "predictions",
        "forecast",
        "trends-to-watch",
    ),
    strong_editorial_tokens=(
        "reported",
        "announced",
        "press release",
        "official statement",
        "filing",
        "earnings",
        "breaking",
        "investigation",
    ),
    weak_signal_tokens=(
        "what to expect",
        "predictions",
        "trends to watch",
        "forecast",
        "outlook",
    ),
    generic_listicle_tokens=(
        "top trends",
        "trends to watch",
        "predictions for",
        "what to expect",
        "top 10 trends",
    ),
)


TOPIC_POLICIES = {
    "consumer_tech": CONSUMER_TECH_POLICY,
    "docs_programming": DOCS_PROGRAMMING_POLICY,
    "news_current": NEWS_CURRENT_POLICY,
}


def detect_topics(text: str) -> set[str]:
    normalized = " ".join(text.lower().split())
    topics: set[str] = set()
    for topic_name, policy in TOPIC_POLICIES.items():
        if any(token in normalized for token in policy.query_tokens):
            topics.add(topic_name)
    return topics


def combined_topics(*texts: str) -> set[str]:
    topics: set[str] = set()
    for text in texts:
        topics.update(detect_topics(text or ""))
    return topics
