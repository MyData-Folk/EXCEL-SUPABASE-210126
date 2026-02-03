import { Command } from 'commander';
import { createClient } from '@supabase/supabase-js';
import { bookingLowestNormalizer } from './normalizers/bookingLowestNormalizer';
import { planningNormalizer } from './normalizers/planningNormalizer';
import { bookingExportNormalizer } from './normalizers/bookingExportNormalizer';
import { eventsNormalizer } from './normalizers/eventsNormalizer';

interface ImportStats {
  rowsInserted: number;
  rowsSkipped: number;
  rowsCoerced: number;
  rowErrors: number;
  errors: string[];
}

const program = new Command();

program
  .requiredOption('--file <path>')
  .requiredOption('--template <template>')
  .requiredOption('--hotel_id <hotel_id>')
  .option('--hotel_name <hotel_name>');

program.parse(process.argv);

const options = program.opts() as {
  file: string;
  template: string;
  hotel_id: string;
  hotel_name?: string;
};

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const batchSize = Number(process.env.BATCH_SIZE ?? 500);
const errorRateThreshold = Number(process.env.ERROR_RATE_THRESHOLD ?? 0.05);

if (!supabaseUrl || !supabaseKey) {
  throw new Error('SUPABASE_URL and SUPABASE_KEY are required');
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function upsertHotel(hotelId: string, hotelName?: string) {
  const payload = { hotel_id: hotelId, hotel_name: hotelName ?? hotelId };
  const { data, error } = await supabase
    .from('hotels')
    .upsert(payload, { onConflict: 'hotel_id' })
    .select('id')
    .single();
  if (error) {
    throw new Error(`Hotel upsert failed: ${error.message}`);
  }
  return data.id as string;
}

async function createImportRun(hotelFk: string, template: string, fileName: string) {
  const { data, error } = await supabase
    .from('import_runs')
    .insert({
      hotel_fk: hotelFk,
      source_file_name: fileName,
      template,
      status: 'running',
      started_at: new Date().toISOString(),
      meta: {}
    })
    .select('id')
    .single();
  if (error) {
    throw new Error(`import_runs insert failed: ${error.message}`);
  }
  return data.id as string;
}

async function updateImportRun(runId: string, payload: Record<string, unknown>) {
  const { error } = await supabase.from('import_runs').update(payload).eq('id', runId);
  if (error) {
    throw new Error(`import_runs update failed: ${error.message}`);
  }
}

async function batchInsert<T extends Record<string, unknown>>(table: string, rows: T[], stats: ImportStats) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const chunk = rows.slice(i, i + batchSize);
    const { error } = await supabase.from(table).insert(chunk);
    if (error) {
      stats.rowErrors += chunk.length;
      stats.errors.push(`${table}: ${error.message}`);
      continue;
    }
    stats.rowsInserted += chunk.length;
  }
}

async function upsertCompetitors(hotelFk: string, competitors: string[]) {
  if (!competitors.length) {
    return;
  }
  const rows = competitors.map((name) => ({
    hotel_fk: hotelFk,
    source: 'booking',
    competitor_name: name
  }));
  await supabase.from('hotel_competitor_set').upsert(rows, {
    onConflict: 'hotel_fk,source,competitor_name'
  });
}

async function run() {
  const stats: ImportStats = {
    rowsInserted: 0,
    rowsSkipped: 0,
    rowsCoerced: 0,
    rowErrors: 0,
    errors: []
  };
  const hotelFk = await upsertHotel(options.hotel_id, options.hotel_name);
  const runId = await createImportRun(hotelFk, options.template, options.file);

  try {
    switch (options.template) {
      case 'planning': {
        const { planningTarifs, disponibilites, stats: planStats } = await planningNormalizer(options.file, hotelFk);
        await batchInsert('planning_tarifs', planningTarifs, stats);
        await batchInsert('disponibilites', disponibilites, stats);
        stats.rowsSkipped += planStats.rowsSkipped;
        stats.rowsCoerced += planStats.rowsCoerced;
        break;
      }
      case 'booking_lowest': {
        const result = await bookingLowestNormalizer(options.file, hotelFk);
        await batchInsert('booking_apercu', result.bookingApercu, stats);
        await batchInsert('booking_tarifs_market', result.bookingTarifsMarket, stats);
        await batchInsert('booking_tarifs_competitors', result.bookingTarifsCompetitors, stats);
        await batchInsert('booking_vs_market', result.bookingVsMarket, stats);
        await batchInsert('booking_vs_competitors', result.bookingVsCompetitors, stats);
        await upsertCompetitors(hotelFk, result.competitors);
        stats.rowsSkipped += result.stats.rowsSkipped;
        stats.rowsCoerced += result.stats.rowsCoerced;
        break;
      }
      case 'booking_export': {
        const { bookingExport, stats: exportStats } = await bookingExportNormalizer(options.file, hotelFk);
        await batchInsert('booking_export', bookingExport, stats);
        stats.rowsSkipped += exportStats.rowsSkipped;
        stats.rowsCoerced += exportStats.rowsCoerced;
        break;
      }
      case 'events': {
        const { events, stats: eventStats } = await eventsNormalizer(options.file, hotelFk);
        await batchInsert('events_calendar', events, stats);
        stats.rowsSkipped += eventStats.rowsSkipped;
        stats.rowsCoerced += eventStats.rowsCoerced;
        break;
      }
      default:
        throw new Error(`Unknown template: ${options.template}`);
    }

    const totalRows = stats.rowsInserted + stats.rowErrors;
    const errorRate = totalRows === 0 ? 0 : stats.rowErrors / totalRows;
    const status = errorRate > errorRateThreshold ? 'failed' : 'success';

    await updateImportRun(runId, {
      status,
      finished_at: new Date().toISOString(),
      error_message: stats.errors.length ? stats.errors.join('; ') : null,
      meta: {
        rows_inserted: stats.rowsInserted,
        rows_skipped: stats.rowsSkipped,
        rows_coerced: stats.rowsCoerced,
        row_errors: stats.rowErrors,
        error_rate: errorRate,
        errors: stats.errors
      }
    });

    console.log(JSON.stringify({
      status,
      rows_inserted: stats.rowsInserted,
      rows_skipped: stats.rowsSkipped,
      rows_coerced: stats.rowsCoerced,
      row_errors: stats.rowErrors,
      error_rate: errorRate
    }, null, 2));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await updateImportRun(runId, {
      status: 'failed',
      finished_at: new Date().toISOString(),
      error_message: message,
      meta: { errors: [message] }
    });
    throw error;
  }
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
