import { DateTime } from 'luxon';

export function excelDateToJS(value: number): Date {
  const excelEpoch = new Date(Date.UTC(1899, 11, 30));
  const days = Math.floor(value);
  const ms = days * 24 * 60 * 60 * 1000;
  const fractional = value - days;
  const timeMs = Math.round(fractional * 24 * 60 * 60 * 1000);
  return new Date(excelEpoch.getTime() + ms + timeMs);
}

export function normalizeDate(value: unknown): string | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return DateTime.fromJSDate(value).toISODate();
  }
  if (typeof value === 'number' && value > 30000) {
    return DateTime.fromJSDate(excelDateToJS(value)).toISODate();
  }
  const text = String(value).trim();
  const dt = DateTime.fromFormat(text, 'dd/MM/yyyy', { zone: 'utc' })
    || DateTime.fromFormat(text, 'dd/MM/yy', { zone: 'utc' });
  if (dt.isValid) {
    return dt.toISODate();
  }
  const parsed = DateTime.fromISO(text);
  return parsed.isValid ? parsed.toISODate() : null;
}

export function splitDateTime(value: unknown): { date: string | null; time: string | null } {
  if (value === null || value === undefined || value === '') {
    return { date: null, time: null };
  }
  let dt: any;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    dt = DateTime.fromJSDate(value);
  } else if (typeof value === 'number' && value > 30000) {
    dt = DateTime.fromJSDate(excelDateToJS(value));
  } else {
    const text = String(value).trim();
    dt = DateTime.fromISO(text);
    if (!dt.isValid) {
      dt = DateTime.fromFormat(text, 'dd/MM/yyyy HH:mm');
    }
    if (!dt.isValid) {
      dt = DateTime.fromFormat(text, 'dd/MM/yyyy HH:mm:ss');
    }
  }
  if (!dt.isValid) {
    return { date: null, time: null };
  }
  return { date: dt.toISODate(), time: dt.toFormat('HH:mm:ss') };
}

export function parseUpdateDate(value: unknown, fallbackYear: number): string | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return DateTime.fromJSDate(value).setZone('Europe/Paris').toISO();
  }
  if (typeof value === 'number' && value > 30000) {
    return DateTime.fromJSDate(excelDateToJS(value)).setZone('Europe/Paris').toISO();
  }
  const text = String(value).replace(/\u00a0/g, ' ').trim();
  const match = text.match(/(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\s*(\d{1,2})?:?(\d{2})?/);
  if (match) {
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = match[3] ? Number(match[3].length === 2 ? `20${match[3]}` : match[3]) : fallbackYear;
    const hour = match[4] ? Number(match[4]) : 0;
    const minute = match[5] ? Number(match[5]) : 0;
    const dt = DateTime.fromObject({ year, month, day, hour, minute }, { zone: 'Europe/Paris' });
    return dt.isValid ? dt.toISO() : null;
  }
  const parsed = DateTime.fromISO(text, { zone: 'Europe/Paris' });
  return parsed.isValid ? parsed.toISO() : null;
}
