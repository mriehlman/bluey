declare const Bun: {
  file(path: string): { json(): Promise<unknown> };
};
