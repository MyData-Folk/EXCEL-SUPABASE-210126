import { Workbook } from 'exceljs';
import { normalizeDate } from '../utils/date';
import { parseNumeric } from '../utils/parseNumeric';
import { findLastColumn, isRowEmpty, readRowValues } from '../utils/excel';

export interface EventRow {
  hotel_fk: string;
  'Événement': string | null;
  'Début': string | null;
  'Fin': string | null;
  'Indice impact attendu sur la demande /10': number | null;
  'Multiplicateur': number | null;
  'Pourquoi cet indice': string | null;
  'Conseils yield': string | null;
  [key: string]: unknown;
}

export async function eventsNormalizer(filePath: string, hotelFk: string) {
  const workbook = new Workbook();
  await workbook.xlsx.readFile(filePath);
  const worksheet = workbook.worksheets[0];
  if (!worksheet) {
    throw new Error('Feuille événements introuvable');
  }
  const headerRow = 1;
  const startCol = 1;
  const endCol = findLastColumn(worksheet, headerRow, startCol);
  const headers = readRowValues(worksheet, headerRow, startCol, endCol).map((value) => String(value ?? '').trim());

  const rows: EventRow[] = [];
  let rowsSkipped = 0;
  let rowsCoerced = 0;

  for (let rowIndex = headerRow + 1; rowIndex <= worksheet.rowCount; rowIndex += 1) {
    const values = readRowValues(worksheet, rowIndex, startCol, endCol);
    if (isRowEmpty(values)) {
      rowsSkipped += 1;
      continue;
    }
    const rowMap: Record<string, unknown> = {};
    headers.forEach((header, idx) => {
      rowMap[header] = values[idx];
    });

    const impact = parseNumeric(rowMap['Indice impact attendu sur la demande /10']);
    const multiplicateur = parseNumeric(rowMap['Multiplicateur']);
    if (impact === null && rowMap['Indice impact attendu sur la demande /10']) {
      rowsCoerced += 1;
    }
    if (multiplicateur === null && rowMap['Multiplicateur']) {
      rowsCoerced += 1;
    }

    rows.push({
      hotel_fk: hotelFk,
      'Événement': rowMap['Événement'] ? String(rowMap['Événement']) : null,
      'Début': normalizeDate(rowMap['Début']),
      'Fin': normalizeDate(rowMap['Fin']),
      'Indice impact attendu sur la demande /10': impact,
      'Multiplicateur': multiplicateur,
      'Pourquoi cet indice': rowMap['Pourquoi cet indice'] ? String(rowMap['Pourquoi cet indice']) : null,
      'Conseils yield': rowMap['Conseils yield'] ? String(rowMap['Conseils yield']) : null
    });
  }

  return { events: rows, stats: { rowsSkipped, rowsCoerced } };
}
