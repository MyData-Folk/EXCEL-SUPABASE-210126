export function parseNumeric(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const text = String(value).replace(/\u00a0/g, ' ').trim();
  if (!text) {
    return null;
  }
  const cleaned = text.replace(/\s+/g, '');
  const hasComma = cleaned.includes(',');
  const hasDot = cleaned.includes('.');
  let normalized = cleaned;
  if (hasComma && hasDot) {
    if (cleaned.lastIndexOf(',') > cleaned.lastIndexOf('.')) {
      normalized = cleaned.replace(/\./g, '').replace(',', '.');
    } else {
      normalized = cleaned.replace(/,/g, '');
    }
  } else if (hasComma) {
    normalized = cleaned.replace(',', '.');
  }
  normalized = normalized.replace(/[^0-9.\-]/g, '');
  if (!normalized || normalized === '-' || normalized === '.' || normalized === '-.') {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseNumericZero(value: unknown): number {
  const parsed = parseNumeric(value);
  return parsed === null ? 0 : parsed;
}
