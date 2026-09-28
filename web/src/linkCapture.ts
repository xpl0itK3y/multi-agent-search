// The first import of main.ts: ES modules run in import order, so this runs before the
// router is created (which records the address) and before the app makes any request.
// It takes an emailed link's one-time token out of the address (lib/linkToken.ts).
import { captureLinkToken } from "@/lib/linkToken";

captureLinkToken();
