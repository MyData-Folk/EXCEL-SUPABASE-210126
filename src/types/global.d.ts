declare const process: {
  env: Record<string, string | undefined>;
  argv: string[];
  exit: (code?: number) => void;
};

declare module 'commander';
declare module '@supabase/supabase-js';
declare module 'exceljs';
declare module 'luxon';
