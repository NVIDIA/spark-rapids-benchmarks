#!/usr/bin/env python3
"""Tests for the update data generation logic in nds_gen_data.py.

Validates:
  1. _check_existing_update_data: detects existing maintenance table data
  2. _merge_update_data_local: merges per-child temp dirs correctly,
     deduplicates delete tables (only keeps first child copy)
  3. _generate_update_data_local: pre-flight safety check blocks overwrite,
     allows with --overwrite_output, uses temp dirs + cleanup
"""

import os
import shutil
import stat
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(__file__))

from nds_gen_data import (
    _check_existing_update_data,
    _merge_update_data_local,
    _generate_update_data_local,
    maintenance_table_names,
)


class TestCheckExistingUpdateData(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_empty_dir_returns_empty(self):
        result = _check_existing_update_data(self.test_dir)
        self.assertEqual(result, [])

    def test_base_data_only_returns_empty(self):
        os.makedirs(os.path.join(self.test_dir, 'customer'))
        Path(os.path.join(self.test_dir, 'customer', 'data.dat')).touch()
        result = _check_existing_update_data(self.test_dir)
        self.assertEqual(result, [])

    def test_detects_existing_update_tables(self):
        for table in ['s_catalog_order', 'delete']:
            table_dir = os.path.join(self.test_dir, table)
            os.makedirs(table_dir)
            Path(os.path.join(table_dir, 'some_data.dat')).touch()
        result = _check_existing_update_data(self.test_dir)
        self.assertIn('s_catalog_order', result)
        self.assertIn('delete', result)

    def test_empty_subfolder_not_detected(self):
        os.makedirs(os.path.join(self.test_dir, 's_inventory'))
        result = _check_existing_update_data(self.test_dir)
        self.assertEqual(result, [])


class TestMergeUpdateDataLocal(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.test_dir, 'data')
        self.temp_base = os.path.join(self.test_dir, 'temp')
        os.makedirs(self.data_dir)
        os.makedirs(self.temp_base)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_child_files(self, child_idx, parallel, update):
        """Simulate dsdgen output for one child."""
        child_dir = os.path.join(self.temp_base, f'child_{child_idx}')
        os.makedirs(child_dir, exist_ok=True)
        created = []
        for table in maintenance_table_names:
            if table in ('delete', 'inventory_delete'):
                fname = f'{table}_{update}.dat'
            else:
                fname = f'{table}_{child_idx}_{parallel}.dat'
            fpath = os.path.join(child_dir, fname)
            with open(fpath, 'w') as f:
                f.write(f'data for {table} child={child_idx}')
            created.append(fpath)
        return created

    def test_regular_tables_merged_from_all_children(self):
        self._create_child_files(1, '4', '1')
        self._create_child_files(2, '4', '1')

        _merge_update_data_local(self.temp_base, self.data_dir, 1, 2, '4', '1')

        target = os.path.join(self.data_dir, 's_catalog_order')
        files = sorted(os.listdir(target))
        self.assertIn('s_catalog_order_1_4.dat', files)
        self.assertIn('s_catalog_order_2_4.dat', files)

    def test_delete_table_only_first_child_kept(self):
        self._create_child_files(1, '4', '2')
        self._create_child_files(2, '4', '2')

        _merge_update_data_local(self.temp_base, self.data_dir, 1, 2, '4', '2')

        delete_dir = os.path.join(self.data_dir, 'delete')
        files = os.listdir(delete_dir)
        self.assertEqual(files, ['delete_2.dat'])
        with open(os.path.join(delete_dir, 'delete_2.dat')) as f:
            content = f.read()
        self.assertIn('child=1', content)

    def test_delete_table_skipped_if_already_exists(self):
        """Simulates a second --range invocation where delete file already exists."""
        delete_dir = os.path.join(self.data_dir, 'delete')
        os.makedirs(delete_dir)
        existing_file = os.path.join(delete_dir, 'delete_1.dat')
        with open(existing_file, 'w') as f:
            f.write('original data from previous range')

        self._create_child_files(3, '4', '1')
        self._create_child_files(4, '4', '1')

        _merge_update_data_local(self.temp_base, self.data_dir, 3, 4, '4', '1')

        with open(existing_file) as f:
            content = f.read()
        self.assertEqual(content, 'original data from previous range')

    def test_creates_table_subdirs(self):
        self._create_child_files(1, '1', '1')
        _merge_update_data_local(self.temp_base, self.data_dir, 1, 1, '1', '1')
        for table in maintenance_table_names:
            self.assertTrue(os.path.isdir(os.path.join(self.data_dir, table)))


class TestGenerateUpdateDataLocal(unittest.TestCase):
    """Test _generate_update_data_local with a mock dsdgen script."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.test_dir, 'output')
        self.tool_dir = os.path.join(self.test_dir, 'tools')
        os.makedirs(self.tool_dir)
        self._create_mock_dsdgen()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _create_mock_dsdgen(self):
        """Create a mock dsdgen that writes expected files based on args."""
        script = textwrap.dedent('''\
            #!/usr/bin/env python3
            import argparse, os
            parser = argparse.ArgumentParser()
            parser.add_argument('-scale')
            parser.add_argument('-dir')
            parser.add_argument('-parallel')
            parser.add_argument('-child')
            parser.add_argument('-verbose')
            parser.add_argument('-update')
            parser.add_argument('-force')
            args = parser.parse_args()

            maintenance_tables = [
                's_catalog_order', 's_catalog_order_lineitem', 's_catalog_returns',
                's_inventory', 's_purchase', 's_purchase_lineitem', 's_store_returns',
                's_web_order', 's_web_order_lineitem', 's_web_returns',
            ]
            delete_tables = ['delete', 'inventory_delete']

            for t in maintenance_tables:
                fname = f'{t}_{args.child}_{args.parallel}.dat'
                with open(os.path.join(args.dir, fname), 'w') as f:
                    f.write(f'{t} child={args.child} scale={args.scale}')

            for t in delete_tables:
                fname = f'{t}_{args.update}.dat'
                with open(os.path.join(args.dir, fname), 'w') as f:
                    f.write(f'{t} update={args.update}')
        ''')
        self.mock_dsdgen = os.path.join(self.tool_dir, 'dsdgen')
        with open(self.mock_dsdgen, 'w') as f:
            f.write(script)
        os.chmod(self.mock_dsdgen, stat.S_IRWXU)

    def _make_args(self, overwrite=False):
        return SimpleNamespace(
            scale='10',
            parallel='2',
            overwrite_output=overwrite,
            update='1',
            data_dir=self.data_dir,
        )

    def test_generates_to_fresh_dir(self):
        args = self._make_args()
        tool_path = Path(self.mock_dsdgen)
        _generate_update_data_local(args, self.data_dir, 1, 2, tool_path)

        self.assertTrue(os.path.isdir(os.path.join(self.data_dir, 's_catalog_order')))
        files = os.listdir(os.path.join(self.data_dir, 's_catalog_order'))
        self.assertIn('s_catalog_order_1_2.dat', files)
        self.assertIn('s_catalog_order_2_2.dat', files)

        delete_files = os.listdir(os.path.join(self.data_dir, 'delete'))
        self.assertEqual(delete_files, ['delete_1.dat'])

    def test_temp_dir_cleaned_up(self):
        args = self._make_args()
        tool_path = Path(self.mock_dsdgen)
        _generate_update_data_local(args, self.data_dir, 1, 2, tool_path)

        temp_path = os.path.join(self.data_dir, '_temp_update')
        self.assertFalse(os.path.exists(temp_path))

    def test_rejects_existing_update_data(self):
        os.makedirs(os.path.join(self.data_dir, 's_catalog_order'))
        Path(os.path.join(self.data_dir, 's_catalog_order', 'old.dat')).touch()

        args = self._make_args(overwrite=False)
        tool_path = Path(self.mock_dsdgen)
        with self.assertRaises(Exception) as ctx:
            _generate_update_data_local(args, self.data_dir, 1, 2, tool_path)
        self.assertIn('Update data already exists', str(ctx.exception))
        self.assertIn('s_catalog_order', str(ctx.exception))

    def test_allows_overwrite_with_flag(self):
        os.makedirs(os.path.join(self.data_dir, 's_catalog_order'))
        Path(os.path.join(self.data_dir, 's_catalog_order', 'old.dat')).touch()

        args = self._make_args(overwrite=True)
        tool_path = Path(self.mock_dsdgen)
        _generate_update_data_local(args, self.data_dir, 1, 2, tool_path)

        files = os.listdir(os.path.join(self.data_dir, 's_catalog_order'))
        self.assertIn('s_catalog_order_1_2.dat', files)

    def test_no_force_flag_passed_to_dsdgen(self):
        """Verify dsdgen is never called with -force (the whole point of this redesign)."""
        script = textwrap.dedent('''\
            #!/usr/bin/env python3
            import sys
            if '-force' in sys.argv:
                sys.exit(99)
            # Generate minimal output so merge doesn't fail
            import argparse, os
            parser = argparse.ArgumentParser()
            parser.add_argument('-scale'); parser.add_argument('-dir')
            parser.add_argument('-parallel'); parser.add_argument('-child')
            parser.add_argument('-verbose'); parser.add_argument('-update')
            args = parser.parse_args()
            for t in ['s_catalog_order']:
                with open(os.path.join(args.dir, f'{t}_{args.child}_{args.parallel}.dat'), 'w') as f:
                    f.write('ok')
            for t in ['delete', 'inventory_delete']:
                with open(os.path.join(args.dir, f'{t}_{args.update}.dat'), 'w') as f:
                    f.write('ok')
        ''')
        with open(self.mock_dsdgen, 'w') as f:
            f.write(script)
        os.chmod(self.mock_dsdgen, stat.S_IRWXU)

        args = self._make_args()
        tool_path = Path(self.mock_dsdgen)
        _generate_update_data_local(args, self.data_dir, 1, 2, tool_path)

    def test_incremental_range_preserves_previous_data(self):
        """Simulate two --range invocations."""
        args = self._make_args()
        tool_path = Path(self.mock_dsdgen)

        # First range: children 1-2
        _generate_update_data_local(args, self.data_dir, 1, 2, tool_path)

        # Second range: children 3-4 (need --overwrite_output since data exists)
        args_range2 = self._make_args(overwrite=True)
        args_range2.parallel = '4'
        _generate_update_data_local(args_range2, self.data_dir, 3, 4, tool_path)

        cat_order_dir = os.path.join(self.data_dir, 's_catalog_order')
        files = sorted(os.listdir(cat_order_dir))
        self.assertIn('s_catalog_order_1_2.dat', files)
        self.assertIn('s_catalog_order_3_4.dat', files)
        self.assertIn('s_catalog_order_4_4.dat', files)

        # Delete file should still be from first range (not overwritten)
        delete_dir = os.path.join(self.data_dir, 'delete')
        with open(os.path.join(delete_dir, 'delete_1.dat')) as f:
            content = f.read()
        self.assertIn('update=1', content)


if __name__ == '__main__':
    unittest.main()
