"""Grouping, keeper and removal rules of dswx_s1_inputs, on hand-built granules (no CMR)."""

import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dswx_s1_inputs as dsi  # noqa: E402


def rtc(burst, acq='20250101T000000Z', created='20250101T010000Z', sensor='S1A', suffix='.h5'):
    return f'OPERA_L2_RTC-S1_{burst}_{acq}_{created}_{sensor}_30_v1.0{suffix}'


def bursts(n, start=0):
    return [f'T001-{100000 + i:06d}-IW1' for i in range(start, start + n)]


def granule(tile, created, start, end=None, sensor='S1A', inputs=(), acq='20250101T000000Z'):
    """Inventory row merged with its signature record, as confirm_sets builds them."""
    signature, raw_signature, keys, non_rtc = dsi.rtc_input_signature(list(inputs))

    return {
        'granule_ur': f'OPERA_L3_DSWx-S1_{tile}_{acq}_{created}_{sensor}_30_v1.0',
        'tile_id': tile, 'sensor': sensor, 'creation_ts': created,
        'time_start': start, 'time_end': end or start,
        'n_input_entries': len(inputs), 'n_bursts': len(keys), 'burst_sig': signature,
        'raw_input_sig': raw_signature, 'n_non_rtc_inputs': len(non_rtc),
        'burst_ids': frozenset(keys),
    }


def t(seconds):
    return f'2025-01-01T00:{seconds // 60:02d}:{seconds % 60:02d}.000Z'


def confirmed(granules, criteria=dsi.CRITERIA_CORRECTED, keeper=dsi.KEEPER_LATEST, tolerance=60):
    candidates = dsi.find_candidates(granules, criteria, tolerance)
    sets, uncached = dsi.confirm_sets(candidates, {g['granule_ur']: g for g in granules}, criteria, keeper)
    assert not uncached

    return [dsi.set_record(group, keeper) for group in sets]


class RtcSignatureTest(unittest.TestCase):
    def test_production_time_and_suffix_are_ignored(self):
        same = [rtc(b, created=c, suffix=s) for b in bursts(2) for c in ('20250101T010000Z', '20250201T010000Z')
                for s in ('.h5', '_VV.tif', '_mask.tif')]
        signature, _, keys, non_rtc = dsi.rtc_input_signature(same)

        self.assertEqual(len(keys), 2)
        self.assertEqual(signature, dsi.rtc_input_signature([rtc(b) for b in bursts(2)])[0])
        self.assertEqual(non_rtc, [])

    def test_acquisition_time_and_sensor_distinguish_bursts(self):
        _, _, keys, _ = dsi.rtc_input_signature([rtc(bursts(1)[0]), rtc(bursts(1)[0], acq='20250113T000000Z'),
                                                 rtc(bursts(1)[0], sensor='S1C')])
        self.assertEqual(len(keys), 3)


class PassGroupingTest(unittest.TestCase):
    def test_chained_starts_form_one_pass(self):
        granules = [granule('T01AAA', '20250101T010000Z', t(s)) for s in (0, 50, 100)]
        self.assertEqual(len(dsi._group_by_pass(granules, 60)), 1)

    def test_separate_passes_and_tiles_and_sensors_split(self):
        granules = [
            granule('T01AAA', '20250101T010000Z', t(0)),
            granule('T01AAA', '20250101T010000Z', '2025-01-01T01:38:04.000Z'),  # next orbit, high latitude
            granule('T01AAB', '20250101T010000Z', t(0)),
            granule('T01AAA', '20250101T010000Z', t(0), sensor='S1C'),
        ]
        self.assertEqual(len(dsi._group_by_pass(granules, 60)), 4)


class DuplicateSetTest(unittest.TestCase):
    def test_identical_inputs(self):
        inputs = [rtc(b) for b in bursts(3)]
        [record] = confirmed([granule('T01AAA', '20250101T010000Z', t(0), inputs=inputs),
                              granule('T01AAA', '20250102T010000Z', t(0), inputs=inputs)])

        self.assertEqual(record['duplicate_kind'], 'identical_inputs')
        self.assertIn('20250102T010000Z', record['keeper'])
        self.assertEqual(len(record['duplicate_products']), 1)

    def test_rtc_relisting(self):
        original = [rtc(b) for b in bursts(3)]
        relisted = original + [rtc(b, created='20250105T000000Z') for b in bursts(3)]
        [record] = confirmed([granule('T01AAA', '20250101T010000Z', t(0), inputs=original),
                              granule('T01AAA', '20250106T010000Z', t(16), inputs=relisted)])

        self.assertEqual(record['duplicate_kind'], 'rtc_relisting')
        self.assertEqual(record['members'][1]['relation_to_keeper'], dsi.RELATION_SAME)

    def test_strict_needs_exact_extent(self):
        inputs = [rtc(b) for b in bursts(3)]
        granules = [granule('T01AAA', '20250101T010000Z', t(0), inputs=inputs),
                    granule('T01AAA', '20250102T010000Z', t(1), inputs=inputs)]

        self.assertEqual(confirmed(granules, dsi.CRITERIA_STRICT), [])
        self.assertEqual(len(confirmed(granules)), 1)

    def test_latest_keeper_with_fewer_bursts_keeps_the_more_complete_copy(self):
        complete = granule('T01AAA', '20250101T010000Z', t(0), inputs=[rtc(b) for b in bursts(6)])
        partial = granule('T01AAA', '20250102T010000Z', t(0), inputs=[rtc(b) for b in bursts(2)])
        [record] = confirmed([complete, partial])

        self.assertEqual(record['keeper'], partial['granule_ur'])
        self.assertTrue(record['keeper_disagreement'])
        self.assertEqual(record['duplicate_products'], [])
        self.assertEqual(record['not_covered_by_keeper'], [complete['granule_ur']])

    def test_most_bursts_keeper_removes_the_subset(self):
        complete = granule('T01AAA', '20250101T010000Z', t(0), inputs=[rtc(b) for b in bursts(6)])
        partial = granule('T01AAA', '20250102T010000Z', t(0), inputs=[rtc(b) for b in bursts(2)])
        [record] = confirmed([complete, partial], keeper=dsi.KEEPER_MOST_BURSTS)

        self.assertEqual(record['keeper'], complete['granule_ur'])
        self.assertEqual(record['duplicate_kind'], 'incomplete_inputs')
        self.assertEqual(record['duplicate_products'], [partial['granule_ur']])

    def test_latest_complete_reprocessing_removes_older_partial(self):
        partial = granule('T01AAA', '20250101T010000Z', t(0), inputs=[rtc(b) for b in bursts(2)])
        complete = granule('T01AAA', '20250301T010000Z', t(0), inputs=[rtc(b) for b in bursts(6)])
        [record] = confirmed([partial, complete])

        self.assertEqual(record['keeper'], complete['granule_ur'])
        self.assertFalse(record['keeper_disagreement'])
        self.assertEqual(record['duplicate_products'], [partial['granule_ur']])

    def test_divergent_overlap_is_never_removed(self):
        left = granule('T01AAA', '20250101T010000Z', t(0), inputs=[rtc(b) for b in bursts(4)])
        right = granule('T01AAA', '20250102T010000Z', t(0), inputs=[rtc(b) for b in bursts(4, start=2)])

        for keeper in dsi.KEEPER_RULES:
            [record] = confirmed([left, right], keeper=keeper)
            self.assertEqual(record['duplicate_kind'], 'divergent_inputs')
            self.assertEqual(record['duplicate_products'], [])
            self.assertEqual(len(record['not_covered_by_keeper']), 1)

    def test_overlap_is_transitive(self):
        a = granule('T01AAA', '20250101T010000Z', t(0), inputs=[rtc(b) for b in bursts(2)])
        b = granule('T01AAA', '20250102T010000Z', t(0), inputs=[rtc(b) for b in bursts(2, start=1)])
        c = granule('T01AAA', '20250103T010000Z', t(0), inputs=[rtc(b) for b in bursts(2, start=2)])

        [record] = confirmed([a, b, c])
        self.assertEqual(len(record['members']), 3)

    def test_disjoint_bursts_in_one_pass_are_not_duplicates(self):
        a = granule('T01AAA', '20250101T010000Z', t(0), inputs=[rtc(b) for b in bursts(2)])
        b = granule('T01AAA', '20250102T010000Z', t(10), inputs=[rtc(b) for b in bursts(2, start=5)])

        self.assertEqual(confirmed([a, b]), [])

    def test_generation_window_limits_removal(self):
        inputs = [rtc(b) for b in bursts(3)]
        group = [granule('T01AAA', '20260101T010000Z', t(0), inputs=inputs),
                 granule('T01AAA', '20250101T010000Z', t(0), inputs=inputs),
                 granule('T01AAA', '20251201T010000Z', t(0), inputs=inputs)]
        record = dsi.set_record(group, dsi.KEEPER_LATEST, generated_before=datetime(2025, 12, 1))

        self.assertEqual(record['duplicate_products'], [group[1]['granule_ur']])
        self.assertEqual(record['outside_generation_window'], [group[2]['granule_ur']])


class ReportSchemaTest(unittest.TestCase):
    def test_name_based_schema_is_kept(self):
        inputs = [rtc(b) for b in bursts(3)]
        granules = [granule('T01AAA', '20250101T010000Z', t(0), inputs=inputs),
                    granule('T01AAA', '20250102T010000Z', t(0), inputs=inputs),
                    granule('T01AAB', '20250101T010000Z', t(0), inputs=inputs)]
        args = SimpleNamespace(product='DSWX_S1', venue='PROD', criteria=dsi.CRITERIA_CORRECTED,
                               keeper=dsi.KEEPER_LATEST, pass_tolerance_seconds=60, start_date=None,
                               end_date=None, generated_after=None, generated_before=None, facet='both')
        records = confirmed(granules)
        report, removal, not_covered = dsi.build_report(args, 'C0', granules, [], records, set(), set(), set(), 0)

        self.assertEqual(report['summary']['n_granules'], 3)
        self.assertEqual(report['summary']['n_duplicates'], 1)
        day = report['dates']['2025-01-01']
        self.assertEqual((day['n_granules'], day['n_duplicates']), (3, 1))
        [entry] = day['duplicates'].values()
        self.assertEqual(entry['latest_product'], entry['keeper'])
        self.assertEqual(entry['duplicate_products'], removal)
        self.assertEqual(not_covered, [])


if __name__ == '__main__':
    unittest.main()
