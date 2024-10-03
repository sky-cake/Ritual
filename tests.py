import unittest

from main import thread_modified, MaxQueue


class TestThreadModified(unittest.TestCase):
    def setUp(self):
        self.d_last_modified = {
            'board1': {
                1001: {'last_modified': 1620500000},
                1002: {'last_modified': 1620600000},
                1003: {'last_modified': 1620700000},
            }
        }

    def test_new_thread(self):
        thread = {'no': 1004, 'last_modified': 1620800000}
        self.assertTrue(thread_modified('board1', thread, self.d_last_modified))

    def test_modified_thread(self):
        thread = {'no': 1002, 'last_modified': 1620650000}
        self.assertTrue(thread_modified('board1', thread, self.d_last_modified))

    def test_unmodified_thread(self):
        thread = {'no': 1002, 'last_modified': 1620600000}
        self.assertFalse(thread_modified('board1', thread, self.d_last_modified))

    def test_missing_board(self):
        thread = {'no': 1004, 'last_modified': 1620800000}
        self.assertTrue(thread_modified('board2', thread, self.d_last_modified))
        self.assertIn(1004, self.d_last_modified['board2'])

    def test_max_entries(self):
        for i in range(4, 205):
            self.d_last_modified['board1'][i] = {'last_modified': 1620800000 + i}

        thread = {'no': 205, 'last_modified': 1620900000}
        self.assertTrue(thread_modified('board1', thread, self.d_last_modified))

        self.assertNotIn(1001, self.d_last_modified['board1'])
        self.assertNotIn(1002, self.d_last_modified['board1'])


class TestMaxQueue(unittest.TestCase):
    def setUp(self):
        self.queue = MaxQueue(boards=["board1", "board2"], max_items_per_board=100)

    def test_add_and_check_existence(self):
        self.queue.add("board1", "file1.jpg")
        self.assertIn("file1.jpg", self.queue["board1"])
        self.assertNotIn("file1.jpg", self.queue["board2"])

    def test_prevent_duplicate_addition(self):
        self.queue.add("board1", "file1.jpg")
        self.queue.add("board1", "file1.jpg")
        self.assertEqual(len(self.queue["board1"]), 1)

    def test_max_capacity(self):
        for i in range(1, 130):
            self.queue.add("board1", f"file{i}.jpg")

        self.assertEqual(len(self.queue["board1"]), 100)

    def test_pop_oldest_item(self):
        for i in range(1, 130):
            self.queue.add("board1", f"file{i}.jpg")

        self.assertNotIn("file1.jpg", self.queue["board1"])
        self.assertNotIn("file2.jpg", self.queue["board1"])

    def test_check_existence_in_different_boards(self):
        self.queue.add("board1", "file1.jpg")
        self.queue.add("board2", "file2.jpg")
        self.assertIn("file1.jpg", self.queue)
        self.assertIn("file2.jpg", self.queue)

    def test_access_items_by_board(self):
        self.queue.add("board1", "file1.jpg")
        self.queue.add("board1", "file2.jpg")
        items = self.queue["board1"]
        self.assertIn("file1.jpg", items)
        self.assertIn("file2.jpg", items)


if __name__ == '__main__':
    unittest.main()
