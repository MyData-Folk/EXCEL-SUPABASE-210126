import { Workbook } from 'exceljs';
import { normalizeDate, splitDateTime } from '../utils/date';
import { parseNumeric } from '../utils/parseNumeric';
import { findLastColumn, isRowEmpty, readRowValues } from '../utils/excel';

export interface BookingExportRow {
  hotel_fk: string;
  [key: string]: unknown;
}

export async function bookingExportNormalizer(filePath: string, hotelFk: string) {
  const workbook = new Workbook();
  await workbook.xlsx.readFile(filePath);
  const worksheet = workbook.worksheets[0];
  if (!worksheet) {
    throw new Error('Feuille BookingExport introuvable');
  }

  const headerRow = 1;
  const startCol = 1;
  const endCol = findLastColumn(worksheet, headerRow, startCol);
  const headers = readRowValues(worksheet, headerRow, startCol, endCol).map((value) => String(value ?? '').trim());

  const datetimeColumns: Record<string, string> = {
    'Date d\'achat': 'Heure d\'achat',
    'Dernière modification': 'Heure de dernière modification',
    'Date d\'annulation': 'Heure d\'annulation'
  };
  const dateColumns = ['Date d\'arrivée', 'Date de départ'];
  const numericColumns = ['Montant total', 'Montant du panier', 'Montant restant'];
  const intColumns = ['Hôtel (ID)', 'Nuits', 'Chambres', 'Adultes', 'Enfants', 'Bébés', 'Garantie', 'Partenaire de distribution (ID)'];

  const rows: BookingExportRow[] = [];
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

    dateColumns.forEach((col) => {
      rowMap[col] = normalizeDate(rowMap[col]);
    });

    Object.entries(datetimeColumns).forEach(([dateCol, timeCol]) => {
      const { date, time } = splitDateTime(rowMap[dateCol]);
      rowMap[dateCol] = date;
      rowMap[timeCol] = time;
    });

    numericColumns.forEach((col) => {
      const parsed = parseNumeric(rowMap[col]);
      if (parsed === null && rowMap[col] !== null && rowMap[col] !== undefined && String(rowMap[col]).trim() !== '') {
        rowsCoerced += 1;
      }
      rowMap[col] = parsed;
    });

    intColumns.forEach((col) => {
      const numeric = parseNumeric(rowMap[col]);
      if (numeric === null && rowMap[col] !== null && rowMap[col] !== undefined && String(rowMap[col]).trim() !== '') {
        rowsCoerced += 1;
      }
      rowMap[col] = numeric === null ? null : Math.trunc(numeric);
    });

    rows.push({ hotel_fk: hotelFk, ...rowMap });
  }

  return { bookingExport: rows, stats: { rowsSkipped, rowsCoerced } };
}
