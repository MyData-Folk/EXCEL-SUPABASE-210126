import { } from 'exceljs';

export function cellToValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'object' && 'text' in value) {
    return (value as { text?: string }).text ?? null;
  }
  if (typeof value === 'object' && 'result' in value) {
    return (value as { result?: unknown }).result ?? null;
  }
  return value as unknown;
}

export function readRowValues(worksheet: any, rowNumber: number, startCol: number, endCol: number): unknown[] {
  const row = worksheet.getRow(rowNumber);
  const values: unknown[] = [];
  for (let col = startCol; col <= endCol; col += 1) {
    values.push(cellToValue(row.getCell(col).value));
  }
  return values;
}

export function findLastColumn(worksheet: any, headerRow: number, startCol: number): number {
  const row = worksheet.getRow(headerRow);
  let last = startCol;
  for (let col = startCol; col <= worksheet.columnCount; col += 1) {
    const value = cellToValue(row.getCell(col).value);
    if (value !== null && value !== undefined && String(value).trim() !== '') {
      last = col;
    }
  }
  return last;
}

export function isRowEmpty(values: unknown[]): boolean {
  return values.every((val) => val === null || val === undefined || String(val).trim() === '');
}
