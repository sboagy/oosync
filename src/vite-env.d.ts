export {};

declare global {
  interface ImportMetaEnv {
    readonly VITE_SYNC_DEBUG?: string;
    readonly VITE_SYNC_DIAGNOSTICS?: string;
    readonly VITE_WORKER_URL?: string;
  }

  interface ImportMeta {
    readonly env: ImportMetaEnv;
  }
}
