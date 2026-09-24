import type { ApiFile } from "./api";

// Hand a downloaded API file to the browser, preferring the server's filename.
export function saveFile(file: ApiFile, fallbackName: string): void {
  const url = URL.createObjectURL(file.blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = file.filename || fallbackName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
