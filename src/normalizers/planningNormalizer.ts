import { Workbook } from 'exceljs';
import { normalizeDate } from '../utils/date';
import { parseNumeric } from '../utils/parseNumeric';
import { cellToValue, findLastColumn, isRowEmpty, readRowValues } from '../utils/excel';

export interface PlanningTarifRow {
  hotel_fk: string;
  date: string;
  type_de_chambre: string;
  plan_tarifaire: string | null;
  tarif: number | null;
  [key: string]: unknown;
}

export interface DisponibiliteRow {
  hotel_fk: string;
  date: string;
  type_de_chambre: string;
  disponibilites: number | null;
  ferme_a_la_vente: string | null;
  [key: string]: unknown;
}

export async function planningNormalizer(filePath: string, hotelFk: string) {
  const workbook = new Workbook();
  await workbook.xlsx.readFile(filePath);
  const worksheet = workbook.getWorksheet('Planning') ?? workbook.worksheets[0];
  if (!worksheet) {
    throw new Error('Sheet Planning introuvable');
  }

  const headerRow = 1;
  const startDateCol = 4;
  const endDateCol = findLastColumn(worksheet, headerRow, startDateCol);
  const dateValues = readRowValues(worksheet, headerRow, startDateCol, endDateCol);
  const dates = dateValues.map((value) => normalizeDate(value));

  const planningTarifs: PlanningTarifRow[] = [];
  const disponibilites: DisponibiliteRow[] = [];
  let rowsSkipped = 0;
  let rowsCoerced = 0;

  let currentRoomType: string | null = null;

  for (let rowIndex = headerRow + 1; rowIndex <= worksheet.rowCount; rowIndex += 1) {
    const row = worksheet.getRow(rowIndex);
    const rowValues = [cellToValue(row.getCell(1).value), cellToValue(row.getCell(2).value), cellToValue(row.getCell(3).value)];
    if (isRowEmpty(rowValues) && isRowEmpty(readRowValues(worksheet, rowIndex, startDateCol, endDateCol))) {
      rowsSkipped += 1;
      continue;
    }
    const roomTypeCell = rowValues[0];
    const planCell = rowValues[1];
    const metricCell = rowValues[2];

    if (roomTypeCell && String(roomTypeCell).trim()) {
      currentRoomType = String(roomTypeCell).trim();
    }
    if (!currentRoomType) {
      rowsSkipped += 1;
      continue;
    }
    const metric = metricCell ? String(metricCell).toLowerCase().replace(/\s+/g, ' ') : '';
    const planTarifaire = planCell ? String(planCell).trim() : null;
    const roomType = currentRoomType;
    if (!roomType) {
      rowsSkipped += 1;
      continue;
    }

    const rowData = readRowValues(worksheet, rowIndex, startDateCol, endDateCol);

    rowData.forEach((cellValue, idx) => {
      const date = dates[idx];
      if (!date) {
        return;
      }
      if (metric.includes('left for sale')) {
        if (cellValue === null || cellValue === undefined || String(cellValue).trim() === '') {
          rowsSkipped += 1;
          return;
        }
        const text = String(cellValue).trim();
        const numericValue = parseNumeric(text);
        if (text.toLowerCase() === 'x') {
          rowsCoerced += 1;
        disponibilites.push({
          hotel_fk: hotelFk,
          date,
          type_de_chambre: roomType,
          disponibilites: 0,
          ferme_a_la_vente: 'x'
        });
          return;
        }
        disponibilites.push({
          hotel_fk: hotelFk,
          date,
          type_de_chambre: roomType,
          disponibilites: numericValue,
          ferme_a_la_vente: null
        });
        return;
      }
      if (metric.includes('price') || metric.includes('tarif')) {
        planningTarifs.push({
          hotel_fk: hotelFk,
          date,
          type_de_chambre: roomType,
          plan_tarifaire: planTarifaire ?? '',
          tarif: parseNumeric(cellValue)
        });
      }
    });
  }

  return { planningTarifs, disponibilites, stats: { rowsSkipped, rowsCoerced } };
}
