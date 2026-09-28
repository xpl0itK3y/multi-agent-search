// The first import of main.ts: ES modules run in import order, so this runs before the
// router is created (which records the address, and whose popstate listener must come
// after ours) and before the app makes any request. It takes an emailed link's one-time
// token out of the address, now and whenever a link is opened again into this page
// (lib/linkToken.ts).
import { captureLinkToken, watchLinkTokens } from "@/lib/linkToken";

captureLinkToken();
watchLinkTokens();
