import { Workbook } from 'exceljs';
import { DateTime } from 'luxon';
import { normalizeDate, parseUpdateDate } from '../utils/date';
import { parseNumeric, parseNumericZero } from '../utils/parseNumeric';
import { cellToValue, findLastColumn, isRowEmpty, readRowValues } from '../utils/excel';

export interface BookingApercuRow {
  hotel_fk: string;
  date_mise_a_jour: string | null;
  'Jour': string | null;
  'Date': string | null;
  'Votre hôtel le plus bas': number;
  'Tarif le plus bas'?: number | null;
  'médiane du compset'?: number | null;
  'Tarif le plus bas, médiane du compset'?: number | null;
  'Classement des tarifs du compset': string | null;
  'Demande du marché': number;
  'Booking.com Classement': string | null;
  'Jours fériés': string | null;
  'Événements': string | null;
  raw_row: Record<string, unknown>;
  [key: string]: unknown;
}

export interface BookingTarifsMarketRow {
  hotel_fk: string;
  date_mise_a_jour: string | null;
  'Jour': string | null;
  'Date': string | null;
  'Demande du marché': number;
  raw_row: Record<string, unknown>;
  [key: string]: unknown;
}

export interface BookingTarifsCompetitorRow {
  hotel_fk: string;
  date_mise_a_jour: string | null;
  'Jour': string | null;
  'Date': string | null;
  competitor_name: string;
  price: number;
  raw_price: string | null;
  [key: string]: unknown;
}

export interface BookingVsMarketRow {
  hotel_fk: string;
  date_mise_a_jour: string | null;
  horizon: string;
  'Jour': string | null;
  'Date': string | null;
  'Demande du marché': number;
  delta_demande: number;
  raw_demande: string | null;
  raw_delta_demande: string | null;
  raw_row: Record<string, unknown>;
  [key: string]: unknown;
}

export interface BookingVsCompetitorRow {
  hotel_fk: string;
  date_mise_a_jour: string | null;
  horizon: string;
  'Jour': string | null;
  'Date': string | null;
  competitor_name: string;
  price: number;
  delta: number;
  raw_price: string | null;
  raw_delta: string | null;
  [key: string]: unknown;
}

const ANNOTATION_PATTERN = /(LOS\s*\d+|LOS\d+|annotation|note)/i;

function rowToObject(headers: string[], values: unknown[]): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  headers.forEach((header, idx) => {
    row[header] = values[idx] ?? null;
  });
  return row;
}

function shouldSkipRow(dateValue: unknown, numericValues: unknown[]): boolean {
  const date = normalizeDate(dateValue);
  if (!date) {
    return true;
  }
  if (numericValues.some((val) => typeof val === 'string' && ANNOTATION_PATTERN.test(val))) {
    return true;
  }
  return false;
}

function extractHeaders(
  worksheet: any,
  headerRow: number,
  startCol: number,
  allowEmpty = false
): { headers: string[]; endCol: number } {
  const endCol = findLastColumn(worksheet, headerRow, startCol);
  const headerValues = readRowValues(worksheet, headerRow, startCol, endCol);
  const headers = headerValues.map((value) => String(value ?? '').trim());
  const finalHeaders = allowEmpty ? headers : headers.filter((value) => value !== '');
  return { headers: finalHeaders, endCol: startCol + finalHeaders.length - 1 };
}

function parseUpdateDateFromSheet(worksheet: any, fallbackYear: number): string | null {
  const g3 = cellToValue(worksheet.getCell('G3').value);
  const g3Parsed = parseUpdateDate(g3, fallbackYear);
  if (g3Parsed) {
    return g3Parsed;
  }
  for (let rowIndex = 1; rowIndex <= 5; rowIndex += 1) {
    const row = worksheet.getRow(rowIndex);
    for (let colIndex = 1; colIndex <= worksheet.columnCount; colIndex += 1) {
      const cell = cellToValue(row.getCell(colIndex).value);
      if (typeof cell === 'string' && cell.toLowerCase().includes('mis')) {
        const candidate = cellToValue(row.getCell(colIndex + 1).value);
        const parsed = parseUpdateDate(candidate, fallbackYear);
        if (parsed) {
          return parsed;
        }
      }
    }
  }
  return null;
}

export async function bookingLowestNormalizer(filePath: string, hotelFk: string) {
  const workbook = new Workbook();
  await workbook.xlsx.readFile(filePath);
  const fallbackYear = DateTime.now().year;

  const tarifsSheet = workbook.getWorksheet('Tarifs');
  const dateMiseAJour = tarifsSheet ? parseUpdateDateFromSheet(tarifsSheet, fallbackYear) : null;

  let rowsSkipped = 0;
  let rowsCoerced = 0;

  const bookingApercu: BookingApercuRow[] = [];
  const bookingTarifsMarket: BookingTarifsMarketRow[] = [];
  const bookingTarifsCompetitors: BookingTarifsCompetitorRow[] = [];
  const bookingVsMarket: BookingVsMarketRow[] = [];
  const bookingVsCompetitors: BookingVsCompetitorRow[] = [];
  const competitors = new Set<string>();

  const apercuSheet = workbook.getWorksheet('Aperçu');
  if (apercuSheet) {
    const headerRow = 5;
    const startCol = 2;
    const { headers, endCol } = extractHeaders(apercuSheet, headerRow, startCol);
    const dataStart = headerRow + 1;

    for (let rowIndex = dataStart; rowIndex <= apercuSheet.rowCount; rowIndex += 1) {
      const values = readRowValues(apercuSheet, rowIndex, startCol, endCol);
      if (isRowEmpty(values)) {
        continue;
      }
      const rawRow = rowToObject(headers, values);
      const rowMap = headers.reduce<Record<string, unknown>>((acc, header, idx) => {
        acc[header] = values[idx];
        return acc;
      }, {});
      const dateValue = rowMap['Date'];
      const numericValues = ['Votre hôtel le plus bas', 'Demande du marché'].map((key) => rowMap[key]);
      if (shouldSkipRow(dateValue, numericValues)) {
        rowsSkipped += 1;
        continue;
      }
      const mergedHeader = headers.find((h) => h.toLowerCase().includes('tarif le plus bas') && h.toLowerCase().includes('médiane'));
      const votreHotel = rowMap['Votre hôtel le plus bas'];
      const demandeMarche = rowMap['Demande du marché'];
      if (parseNumeric(votreHotel) === null && votreHotel !== null && votreHotel !== undefined && String(votreHotel).trim() !== '') {
        rowsCoerced += 1;
      }
      if (parseNumeric(demandeMarche) === null && demandeMarche !== null && demandeMarche !== undefined && String(demandeMarche).trim() !== '') {
        rowsCoerced += 1;
      }
      bookingApercu.push({
        hotel_fk: hotelFk,
        date_mise_a_jour: dateMiseAJour,
        'Jour': rowMap['Jour'] ? String(rowMap['Jour']) : null,
        'Date': normalizeDate(dateValue),
        'Votre hôtel le plus bas': parseNumericZero(votreHotel),
        'Tarif le plus bas': rowMap['Tarif le plus bas'] !== undefined ? parseNumeric(rowMap['Tarif le plus bas']) : null,
        'médiane du compset': rowMap['médiane du compset'] !== undefined ? parseNumeric(rowMap['médiane du compset']) : null,
        'Tarif le plus bas, médiane du compset': mergedHeader ? parseNumeric(rowMap[mergedHeader]) : null,
        'Classement des tarifs du compset': rowMap['Classement des tarifs du compset'] ? String(rowMap['Classement des tarifs du compset']) : null,
        'Demande du marché': parseNumericZero(demandeMarche),
        'Booking.com Classement': rowMap['Booking.com Classement'] ? String(rowMap['Booking.com Classement']) : null,
        'Jours fériés': rowMap['Jours fériés'] ? String(rowMap['Jours fériés']) : null,
        'Événements': rowMap['Événements'] ? String(rowMap['Événements']) : null,
        raw_row: rawRow
      });
    }
  }

  if (tarifsSheet) {
    const headerRow = 5;
    const startCol = 2;
    const { headers, endCol } = extractHeaders(tarifsSheet, headerRow, startCol);
    const dataStart = headerRow + 1;

    for (let rowIndex = dataStart; rowIndex <= tarifsSheet.rowCount; rowIndex += 1) {
      const values = readRowValues(tarifsSheet, rowIndex, startCol, endCol);
      if (isRowEmpty(values)) {
        continue;
      }
      const rawRow = rowToObject(headers, values);
      const rowMap = headers.reduce<Record<string, unknown>>((acc, header, idx) => {
        acc[header] = values[idx];
        return acc;
      }, {});
      const dateValue = rowMap['Date'];
      if (shouldSkipRow(dateValue, Object.values(rowMap))) {
        rowsSkipped += 1;
        continue;
      }
      const jour = rowMap['Jour'] ? String(rowMap['Jour']) : null;
      const date = normalizeDate(dateValue);
      const demandeValue = rowMap['Demande du marché'];
      if (parseNumeric(demandeValue) === null && demandeValue !== null && demandeValue !== undefined && String(demandeValue).trim() !== '') {
        rowsCoerced += 1;
      }
      const demande = parseNumericZero(demandeValue);
      bookingTarifsMarket.push({
        hotel_fk: hotelFk,
        date_mise_a_jour: dateMiseAJour,
        'Jour': jour,
        'Date': date,
        'Demande du marché': demande,
        raw_row: rawRow
      });
      headers.forEach((header) => {
        if (['Jour', 'Date', 'Demande du marché'].includes(header)) {
          return;
        }
        const rawPrice = rowMap[header];
        const parsed = parseNumeric(rawPrice);
        if (parsed === null && rawPrice !== null && rawPrice !== undefined && String(rawPrice).trim() !== '') {
          rowsCoerced += 1;
        }
        const numeric = parseNumericZero(rawPrice);
        const rawText = rawPrice !== null && rawPrice !== undefined ? String(rawPrice) : null;
        competitors.add(header);
        bookingTarifsCompetitors.push({
          hotel_fk: hotelFk,
          date_mise_a_jour: dateMiseAJour,
          'Jour': jour,
          'Date': date,
          competitor_name: header,
          price: numeric,
          raw_price: rawText
        });
      });
    }
  }

  const vsSheets = [
    { name: 'vs. Hier', horizon: 'hier' },
    { name: 'vs. 3 jours', horizon: '3j' },
    { name: 'vs. 7 jours', horizon: '7j' }
  ];

  for (const vsSheetConfig of vsSheets) {
    const sheet = workbook.getWorksheet(vsSheetConfig.name);
    if (!sheet) {
      continue;
    }
    const headerRow = 5;
    const startCol = 2;
    const { headers, endCol } = extractHeaders(sheet, headerRow, startCol, true);
    const dataStart = headerRow + 1;
    const reconstructedHeaders: string[] = [];
    let lastHeader = '';
    headers.forEach((header) => {
      if (header === '' && lastHeader) {
        reconstructedHeaders.push(`${lastHeader}__delta`);
      } else {
        reconstructedHeaders.push(header);
        lastHeader = header;
      }
    });

    for (let rowIndex = dataStart; rowIndex <= sheet.rowCount; rowIndex += 1) {
      const values = readRowValues(sheet, rowIndex, startCol, endCol);
      if (isRowEmpty(values)) {
        continue;
      }
      const rawRow = rowToObject(reconstructedHeaders, values);
      const rowMap = reconstructedHeaders.reduce<Record<string, unknown>>((acc, header, idx) => {
        acc[header] = values[idx];
        return acc;
      }, {});
      if (shouldSkipRow(rowMap['Date'], Object.values(rowMap))) {
        rowsSkipped += 1;
        continue;
      }
      const jour = rowMap['Jour'] ? String(rowMap['Jour']) : null;
      const date = normalizeDate(rowMap['Date']);
      const demandeValue = rowMap['Demande du marché'];
      const deltaDemandeValue = rowMap['Demande du marché__delta'];
      if (parseNumeric(demandeValue) === null && demandeValue !== null && demandeValue !== undefined && String(demandeValue).trim() !== '') {
        rowsCoerced += 1;
      }
      if (parseNumeric(deltaDemandeValue) === null && deltaDemandeValue !== null && deltaDemandeValue !== undefined && String(deltaDemandeValue).trim() !== '') {
        rowsCoerced += 1;
      }
      const demande = parseNumericZero(demandeValue);
      const deltaDemande = parseNumericZero(deltaDemandeValue);
      bookingVsMarket.push({
        hotel_fk: hotelFk,
        date_mise_a_jour: dateMiseAJour,
        horizon: vsSheetConfig.horizon,
        'Jour': jour,
        'Date': date,
        'Demande du marché': demande,
        delta_demande: deltaDemande,
        raw_demande: demandeValue !== null && demandeValue !== undefined ? String(demandeValue) : null,
        raw_delta_demande: deltaDemandeValue !== null && deltaDemandeValue !== undefined ? String(deltaDemandeValue) : null,
        raw_row: rawRow
      });
      reconstructedHeaders.forEach((header) => {
        if (['Jour', 'Date', 'Demande du marché', 'Demande du marché__delta'].includes(header)) {
          return;
        }
        if (header.endsWith('__delta')) {
          return;
        }
        const deltaHeader = `${header}__delta`;
        const rawPrice = rowMap[header];
        const rawDelta = rowMap[deltaHeader];
        if (parseNumeric(rawPrice) === null && rawPrice !== null && rawPrice !== undefined && String(rawPrice).trim() !== '') {
          rowsCoerced += 1;
        }
        if (parseNumeric(rawDelta) === null && rawDelta !== null && rawDelta !== undefined && String(rawDelta).trim() !== '') {
          rowsCoerced += 1;
        }
        const price = parseNumericZero(rawPrice);
        const delta = parseNumericZero(rawDelta);
        const rawPriceText = rawPrice !== null && rawPrice !== undefined ? String(rawPrice) : null;
        const rawDeltaText = rawDelta !== null && rawDelta !== undefined ? String(rawDelta) : null;
        competitors.add(header);
        bookingVsCompetitors.push({
          hotel_fk: hotelFk,
          date_mise_a_jour: dateMiseAJour,
          horizon: vsSheetConfig.horizon,
          'Jour': jour,
          'Date': date,
          competitor_name: header,
          price,
          delta,
          raw_price: rawPriceText,
          raw_delta: rawDeltaText
        });
      });
    }
  }

  return {
    dateMiseAJour,
    bookingApercu,
    bookingTarifsMarket,
    bookingTarifsCompetitors,
    bookingVsMarket,
    bookingVsCompetitors,
    competitors: Array.from(competitors),
    stats: { rowsSkipped, rowsCoerced }
  };
}
